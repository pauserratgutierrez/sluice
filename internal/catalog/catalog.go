// Package catalog reads and caches the PostgreSQL metadata Sluice needs to
// authorize: RLS policies, column-level grants, replica identity, and index
// coverage.
//
// Relation metadata is cached and refreshed on SLUICE_CATALOG_REFRESH and after
// a schema change arrives on the replication stream; column privileges are
// queried at subscribe time and kept until the next refresh. None of it is on
// the per-change path.
package catalog

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pauserratgutierrez/sluice/internal/encode"
	"github.com/pauserratgutierrez/sluice/internal/expr"
)

// Policy is one RLS policy relevant to SELECT.
type Policy struct {
	Name       string
	Permissive bool
	Roles      []string // empty means PUBLIC
	Using      string   // pg_get_expr(polqual, polrelid)
	Parsed     expr.Node
	ParseErr   string
}

// Relation is the cached authorization metadata for one table.
type Relation struct {
	OID        uint32
	Schema     string
	Name       string
	RLSEnabled bool
	// ReplicaIdentity is pg_class.relreplident: 'd', 'n', 'f' or 'i'.
	ReplicaIdentity byte
	// ReplicaIdentityColumns are the columns guaranteed to appear in an old
	// tuple. A shape filtering on anything outside this set cannot evaluate
	// DELETE events, which is the single most common misconfiguration.
	ReplicaIdentityColumns []string
	// ReplicaIdentityOK is false when relreplident is 'i' but the named index no
	// longer exists, or 'd' with no primary key. In that state the APPLICATION's
	// UPDATE and DELETE statements fail, so it is a fatal condition, not a
	// Sluice-only problem.
	ReplicaIdentityOK bool
	// KeyColumns identify a row: the replica identity index for USING INDEX,
	// the primary key otherwise (including FULL, where every column is in the
	// replica identity but only the key identifies the row). Empty when the
	// table has neither.
	KeyColumns         []string
	Columns            []Column
	IndexedColumns     map[string]bool
	Policies           []Policy
	HasToastableColumn bool
}

type Column struct {
	Name     string
	TypeName string
	TypeOID  uint32
	AttNum   int16
	NotNull  bool
}

// FullName returns schema.table.
//
// Deliberately recomputed rather than memoised. A lazily-populated cache field
// is a data race the moment two goroutines read it, which is the normal case
// here: dispatch and /diagnostics both walk relations concurrently. The race
// detector caught exactly that. Concatenating two short strings is cheaper than
// the mutex that would make caching safe, and this is not on the per-change path.
func (r *Relation) FullName() string { return r.Schema + "." + r.Name }

// Column looks up a column by name.
func (r *Relation) Column(name string) (Column, bool) {
	for _, c := range r.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// InReplicaIdentity reports whether a column survives into old tuples.
func (r *Relation) InReplicaIdentity(name string) bool {
	if r.ReplicaIdentity == 'f' {
		return true
	}
	for _, c := range r.ReplicaIdentityColumns {
		if c == name {
			return true
		}
	}
	return false
}

// Predicate builds the combined SELECT predicate for a role: permissive
// policies OR'd, restrictive policies AND'ed. That combination order is not
// cosmetic -- getting it backwards would turn a restriction into a grant.
//
// memberOf must come from Cache.MemberOf and roleBypassesRLS from
// Cache.BypassesRLS. They are parameters rather than lookups because a Relation
// is a plain snapshot with no way back to the cache. A policy applies to every
// role that has the privileges of one of its TO roles, exactly as PostgreSQL
// checks it; missing a membership would drop a RESTRICTIVE policy and widen
// access.
//
// The second return value carries a reason when a policy could not be parsed,
// which forces Tier C rather than silently dropping the restriction.
//
// The third is the same predicate as raw SQL, assembled from PostgreSQL's own
// pg_get_expr output rather than re-rendered from the AST. That distinction
// matters: it is the text handed back to PostgreSQL for Tier C evaluation and
// for the Tier B soundness cross-check, so it must be PostgreSQL's own spelling,
// not Sluice's approximation of it.
func (r *Relation) Predicate(role string, memberOf map[string]bool, roleBypassesRLS bool) (expr.Node, string, string) {
	// PostgreSQL's own rule, from check_enable_rls(): a relation with RLS off,
	// or a role holding BYPASSRLS, sees every row. FORCE ROW LEVEL SECURITY
	// does not claw that back -- it only subjects the table's OWNER to RLS.
	//
	// The one bypass PostgreSQL grants that Sluice does not model is the
	// owner's: an owner of a table without FORCE also sees every row. Deciding
	// that faithfully means expanding role membership (PostgreSQL checks
	// has_privs_of_role, not role identity), so it is left fail-closed. In a
	// Supabase layout the owner is `postgres`, which is a superuser and so
	// already bypasses above.
	if !r.RLSEnabled || roleBypassesRLS {
		return expr.TrueNode, "", "true"
	}
	var permissive, restrictive []expr.Node
	var permSQL, restSQL []string
	var reason string

	for _, p := range r.Policies {
		if !policyAppliesTo(p, role, memberOf) {
			continue
		}
		node := p.Parsed
		if node == nil {
			if reason == "" {
				reason = fmt.Sprintf("policy %q could not be parsed: %s", p.Name, p.ParseErr)
			}
			// A policy we cannot parse must not be silently ignored; represent it
			// as something the compiler is guaranteed to reject.
			node = &expr.SubqueryExpr{SQL: p.Using}
		}
		if p.Permissive {
			permissive = append(permissive, node)
			permSQL = append(permSQL, "("+p.Using+")")
		} else {
			restrictive = append(restrictive, node)
			restSQL = append(restSQL, "("+p.Using+")")
		}
	}

	// No applicable permissive policy means no visible rows, which is
	// PostgreSQL's default-deny once RLS is on.
	sql := "false"
	if len(permSQL) > 0 {
		sql = "(" + strings.Join(permSQL, " OR ") + ")"
	}
	if len(restSQL) > 0 {
		sql += " AND (" + strings.Join(restSQL, " AND ") + ")"
	}
	return expr.And(expr.Or(permissive...), expr.And(restrictive...)), reason, sql
}

func policyAppliesTo(p Policy, role string, memberOf map[string]bool) bool {
	if len(p.Roles) == 0 {
		return true // PUBLIC
	}
	for _, r := range p.Roles {
		if r == role || r == "public" || memberOf[r] {
			return true
		}
	}
	return false
}

// Cache holds relation metadata and refreshes it.
type Cache struct {
	pool *pgxpool.Pool
	// roles are the JWT roles Sluice accepts; their memberships are loaded so
	// policies written for a parent role apply to them.
	roles []string

	mu       sync.RWMutex
	byOID    map[uint32]*Relation
	byName   map[string]*Relation
	bypass   map[string]bool
	memberOf map[string]map[string]bool
	types    map[uint32]*encode.Type
	loaded   time.Time

	// authzFP fingerprints everything an authorization decision reads, and
	// authzVer counts the times it changed. See AuthzVersion.
	authzFP  [32]byte
	authzVer uint64

	// privileges caches column privilege answers until the next Refresh, which
	// bumps privGen. See columnPrivileges.
	privMu     sync.Mutex
	privileges map[privilegeKey]bool
	privGen    uint64
}

// New creates an empty cache. roles are the JWT roles whose memberships
// Predicate needs (SLUICE_ALLOWED_ROLES).
func New(pool *pgxpool.Pool, roles ...string) *Cache {
	return &Cache{
		pool:     pool,
		roles:    roles,
		byOID:    map[uint32]*Relation{},
		byName:   map[string]*Relation{},
		bypass:   map[string]bool{},
		memberOf: map[string]map[string]bool{},
	}
}

func (c *Cache) Get(oid uint32) (*Relation, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	r, ok := c.byOID[oid]
	return r, ok
}

func (c *Cache) Lookup(schema, name string) (*Relation, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	r, ok := c.byName[schema+"."+name]
	return r, ok
}

// All returns a snapshot of every cached relation, sorted by name.
func (c *Cache) All() []*Relation {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*Relation, 0, len(c.byOID))
	for _, r := range c.byOID {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FullName() < out[j].FullName() })
	return out
}

// PutForTest installs relations into the cache. Tests only; not a product API.
func (c *Cache) PutForTest(rels ...*Relation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byOID == nil {
		c.byOID = map[uint32]*Relation{}
	}
	if c.byName == nil {
		c.byName = map[string]*Relation{}
	}
	for _, r := range rels {
		if r == nil {
			continue
		}
		c.byOID[r.OID] = r
		c.byName[r.FullName()] = r
	}
	c.loaded = time.Now()
}

const relationQuery = `
SELECT c.oid::oid,
       n.nspname,
       c.relname,
       c.relrowsecurity,
       c.relreplident,
       COALESCE((
         SELECT array_agg(a.attname ORDER BY a.attnum)
         FROM pg_index i
         JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
         WHERE i.indrelid = c.oid
           AND ((c.relreplident = 'd' AND i.indisprimary)
             OR (c.relreplident = 'i' AND i.indisreplident))
       ), '{}')::text[] AS ri_columns,
       CASE c.relreplident
         WHEN 'f' THEN true
         WHEN 'd' THEN EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary)
         WHEN 'i' THEN EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisreplident)
         ELSE false
       END AS ri_ok,
       COALESCE((
         SELECT array_agg(a.attname ORDER BY array_position(i.indkey::int2[], a.attnum))
         FROM pg_index i
         JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
         WHERE i.indrelid = c.oid
           AND ((c.relreplident = 'i' AND i.indisreplident)
             OR (c.relreplident <> 'i' AND i.indisprimary))
       ), '{}')::text[] AS key_columns,
       COALESCE((
         SELECT array_agg(a.attname ORDER BY a.attnum)
         FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
       ), '{}')::text[] AS col_names,
       COALESCE((
         SELECT array_agg(format_type(a.atttypid, a.atttypmod) ORDER BY a.attnum)
         FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
       ), '{}')::text[] AS col_types,
       COALESCE((
         SELECT array_agg(a.atttypid ORDER BY a.attnum)
         FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
       ), '{}')::oid[] AS col_type_oids,
       COALESCE((
         SELECT array_agg(a.attnum ORDER BY a.attnum)
         FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
       ), '{}')::smallint[] AS col_nums,
       COALESCE((
         SELECT array_agg(a.attnotnull ORDER BY a.attnum)
         FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
       ), '{}')::bool[] AS col_notnull,
       -- any leading indexed column is a candidate routing key
       COALESCE((
         SELECT array_agg(DISTINCT a.attname)
         FROM pg_index i
         JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = i.indkey[0]
         WHERE i.indrelid = c.oid AND i.indexprs IS NULL
       ), '{}')::text[] AS indexed_columns,
       -- a TOAST-able column makes REPLICA IDENTITY FULL expensive
       EXISTS (SELECT 1 FROM pg_attribute a
                WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                  AND a.attstorage IN ('x','e')) AS has_toastable
FROM pg_publication_tables pt
JOIN pg_namespace n ON n.nspname = pt.schemaname
JOIN pg_class c ON c.relname = pt.tablename AND c.relnamespace = n.oid
WHERE pt.pubname = $1`

// typeQuery loads every type a published column uses, and everything those
// types are built from: a domain's base type, an array's element type, a
// composite's attribute types. encodeTypes resolves them.
const typeQuery = `
WITH RECURSIVE walk(oid) AS (
  SELECT DISTINCT a.atttypid
    FROM pg_publication_tables pt
    JOIN pg_namespace n ON n.nspname = pt.schemaname
    JOIN pg_class c ON c.relname = pt.tablename AND c.relnamespace = n.oid
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
   WHERE pt.pubname = $1
  UNION
  SELECT next.oid
    FROM walk w
    JOIN pg_type t ON t.oid = w.oid
    CROSS JOIN LATERAL (
      SELECT t.typbasetype AS oid WHERE t.typtype = 'd'
      UNION ALL
      SELECT t.typelem WHERE t.typcategory = 'A' AND t.typelem <> 0
      UNION ALL
      SELECT a.atttypid FROM pg_attribute a
       WHERE t.typtype = 'c' AND a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped
    ) next
)
SELECT t.oid, t.typtype, t.typcategory, t.typdelim, t.typbasetype, t.typelem,
       COALESCE((SELECT array_agg(a.attname ORDER BY a.attnum) FROM pg_attribute a
                  WHERE t.typtype = 'c' AND a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped),
                '{}')::text[],
       COALESCE((SELECT array_agg(a.atttypid ORDER BY a.attnum) FROM pg_attribute a
                  WHERE t.typtype = 'c' AND a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped),
                '{}')::oid[]
  FROM walk w JOIN pg_type t ON t.oid = w.oid`

type typeRow struct {
	typtype, category, delim byte
	base, elem               uint32
	fieldNames               []string
	fieldTypes               []uint32
}

// encodeTypes resolves each loaded type to how to_jsonb encodes it: a domain
// as its base type, an array by its element type, a composite by its
// attributes, and a base type by its OID.
func encodeTypes(rows map[uint32]typeRow) map[uint32]*encode.Type {
	out := make(map[uint32]*encode.Type, len(rows))
	var resolve func(oid uint32, depth int) *encode.Type
	resolve = func(oid uint32, depth int) *encode.Type {
		if t, ok := out[oid]; ok {
			return t
		}
		r, ok := rows[oid]
		if !ok || depth > 32 {
			return encode.Builtin(oid)
		}
		var t *encode.Type
		switch {
		case r.typtype == 'd':
			t = resolve(r.base, depth+1)
		case r.category == 'A' && r.elem != 0:
			delim := rows[r.elem].delim
			if delim == 0 {
				delim = ','
			}
			t = &encode.Type{Kind: encode.Array, Elem: resolve(r.elem, depth+1), Delim: delim}
		case r.typtype == 'c':
			t = &encode.Type{Kind: encode.Composite}
			for i, name := range r.fieldNames {
				if i < len(r.fieldTypes) {
					t.Fields = append(t.Fields, encode.Field{Name: name, Type: resolve(r.fieldTypes[i], depth+1)})
				}
			}
		default:
			t = encode.Builtin(oid)
		}
		out[oid] = t
		return t
	}
	for oid := range rows {
		resolve(oid, 0)
	}
	return out
}

func loadTypes(ctx context.Context, q querier, publication string) (map[uint32]*encode.Type, error) {
	rows, err := q.Query(ctx, typeQuery, publication)
	if err != nil {
		return nil, fmt.Errorf("catalog: query column types: %w", err)
	}
	defer rows.Close()
	loaded := map[uint32]typeRow{}
	for rows.Next() {
		var oid uint32
		var r typeRow
		if err := rows.Scan(&oid, &r.typtype, &r.category, &r.delim, &r.base, &r.elem,
			&r.fieldNames, &r.fieldTypes); err != nil {
			return nil, fmt.Errorf("catalog: scan column type: %w", err)
		}
		loaded[oid] = r
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: iterate column types: %w", err)
	}
	return encodeTypes(loaded), nil
}

// Types maps a column type OID to its wire encoding. The map is replaced, not
// modified, on every refresh, so callers may keep and read it without a lock.
// A type missing from it (one created since the last refresh) is encoded by
// encode.Builtin.
func (c *Cache) Types() map[uint32]*encode.Type {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.types
}

// memberQuery lists, for each accepted JWT role, the roles whose privileges it
// has. pg_has_role(..., 'USAGE') is has_privs_of_role, the same test PostgreSQL
// applies to a policy's TO list.
const memberQuery = `
SELECT r.rolname, g.rolname
  FROM pg_roles r
  JOIN pg_roles g ON g.oid <> r.oid AND pg_has_role(r.oid, g.oid, 'USAGE')
 WHERE r.rolname = ANY($1::text[])`

// bypassQuery lists the roles for which RLS is not enforced at all.
//
// This mirrors PostgreSQL's has_bypassrls_privilege(): the attribute itself, or
// superuser, which implies it. Deliberately NOT a membership query --
// PostgreSQL reads rolbypassrls off the role that is current, so a role merely
// granted membership in a BYPASSRLS role does not inherit the bypass.
const bypassQuery = `SELECT rolname FROM pg_roles WHERE rolbypassrls OR rolsuper`

const policyQuery = `
SELECT p.polrelid::oid,
       p.polname,
       p.polpermissive,
       COALESCE((SELECT array_agg(pg_get_userbyid(r) ) FROM unnest(p.polroles) AS r
                  WHERE r <> 0), '{}')::text[] AS roles,
       COALESCE(pg_get_expr(p.polqual, p.polrelid), '') AS using_expr
FROM pg_policy p
WHERE p.polcmd IN ('r','*') AND p.polqual IS NOT NULL
  AND p.polrelid = ANY($1::oid[])`

// querier is what the catalog loaders need from a connection or transaction.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Refresh reloads every published relation.
func (c *Cache) Refresh(ctx context.Context, publication string) error {
	// JIT is off for these queries: they read a few catalog rows, but the
	// recursive type walk is estimated far above jit_above_cost, and compiling
	// it takes hundreds of milliseconds for a query that runs in a few.
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("catalog: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL jit = off`); err != nil {
		return fmt.Errorf("catalog: disable jit: %w", err)
	}

	bypass, err := c.loadBypassRoles(ctx, tx)
	if err != nil {
		return err
	}
	memberOf, err := c.loadMemberships(ctx, tx)
	if err != nil {
		return err
	}
	types, err := loadTypes(ctx, tx, publication)
	if err != nil {
		return err
	}

	rows, err := tx.Query(ctx, relationQuery, publication)
	if err != nil {
		return fmt.Errorf("catalog: query relations: %w", err)
	}
	defer rows.Close()

	byOID := map[uint32]*Relation{}
	byName := map[string]*Relation{}
	var oids []uint32

	for rows.Next() {
		var (
			r         Relation
			riCols    []string
			keyCols   []string
			colNames  []string
			colTypes  []string
			colOIDs   []uint32
			colNums   []int16
			colNotNul []bool
			indexed   []string
		)
		if err := rows.Scan(&r.OID, &r.Schema, &r.Name, &r.RLSEnabled, &r.ReplicaIdentity,
			&riCols, &r.ReplicaIdentityOK, &keyCols, &colNames, &colTypes, &colOIDs, &colNums, &colNotNul,
			&indexed, &r.HasToastableColumn); err != nil {
			return fmt.Errorf("catalog: scan relation: %w", err)
		}
		r.ReplicaIdentityColumns = riCols
		r.KeyColumns = keyCols
		r.IndexedColumns = make(map[string]bool, len(indexed))
		for _, ic := range indexed {
			r.IndexedColumns[ic] = true
		}
		for i := range colNames {
			col := Column{Name: colNames[i]}
			if i < len(colTypes) {
				col.TypeName = colTypes[i]
			}
			if i < len(colOIDs) {
				col.TypeOID = colOIDs[i]
			}
			if i < len(colNums) {
				col.AttNum = colNums[i]
			}
			if i < len(colNotNul) {
				col.NotNull = colNotNul[i]
			}
			r.Columns = append(r.Columns, col)
		}
		rr := r
		byOID[r.OID] = &rr
		byName[r.FullName()] = &rr
		oids = append(oids, r.OID)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("catalog: iterate relations: %w", err)
	}

	if len(oids) > 0 {
		prows, err := tx.Query(ctx, policyQuery, oids)
		if err != nil {
			return fmt.Errorf("catalog: query policies: %w", err)
		}
		defer prows.Close()
		for prows.Next() {
			var (
				oid uint32
				p   Policy
			)
			if err := prows.Scan(&oid, &p.Name, &p.Permissive, &p.Roles, &p.Using); err != nil {
				return fmt.Errorf("catalog: scan policy: %w", err)
			}
			rel := byOID[oid]
			if rel == nil {
				continue
			}
			// Parse now, once, rather than per subscription. A parse failure is
			// recorded rather than fatal: it forces Tier C, which is correct but
			// slow, and it is surfaced in /diagnostics.
			node, perr := expr.Parse(p.Using)
			if perr != nil {
				p.ParseErr = perr.Error()
			} else {
				p.Parsed = node
			}
			sort.Strings(p.Roles)
			rel.Policies = append(rel.Policies, p)
		}
		if err := prows.Err(); err != nil {
			return fmt.Errorf("catalog: iterate policies: %w", err)
		}
	}

	for _, rel := range byOID {
		rel.sortPolicies()
	}
	fp := authzFingerprint(byOID, bypass, memberOf)

	c.mu.Lock()
	if fp != c.authzFP {
		c.authzFP = fp
		c.authzVer++
	}
	c.byOID, c.byName, c.bypass, c.memberOf, c.types, c.loaded = byOID, byName, bypass, memberOf, types, time.Now()
	c.mu.Unlock()
	c.forgetPrivileges()
	return nil
}

func (c *Cache) forgetPrivileges() {
	c.privMu.Lock()
	c.privileges = nil
	c.privGen++
	c.privMu.Unlock()
}

// sortPolicies puts the policies in a canonical order, because neither the
// authorization fingerprint nor PredicateSQL -- which is assembled in this
// order -- should depend on the order pg_policy rows came back in. Unstable
// PredicateSQL would churn the fingerprint on every refresh and re-resolve
// every subscription for nothing.
func (r *Relation) sortPolicies() {
	sort.Slice(r.Policies, func(i, j int) bool { return r.Policies[i].Name < r.Policies[j].Name })
}

// AuthzVersion changes whenever anything an authorization decision reads
// changes: RLS flags, SELECT policies, role memberships, or the set of roles
// that bypass RLS.
//
// It exists because a decision is resolved once, at subscribe time, and most
// decisions carry no lease -- a stable Tier A or Tier B predicate is re-read
// only when the catalog moves. DROP POLICY and ALTER ROLE ... NOBYPASSRLS emit
// no WAL Relation message, so without this the periodic refresh would load the
// revocation into the cache and nothing would ever act on it.
//
// It is a counter over a content hash rather than a bump per refresh, so a
// refresh that changed nothing costs nothing.
func (c *Cache) AuthzVersion() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.authzVer
}

// authzFingerprint hashes exactly the inputs Predicate and Resolve read. Fields
// are length-prefixed so that no combination of policy, role or relation names
// can be rearranged into the same byte stream.
func authzFingerprint(byOID map[uint32]*Relation, bypass map[string]bool, memberOf map[string]map[string]bool) [32]byte {
	h := sha256.New()
	var num [8]byte
	put := func(s string) {
		binary.LittleEndian.PutUint64(num[:], uint64(len(s)))
		h.Write(num[:])
		io.WriteString(h, s)
	}
	putUint := func(v uint64) {
		binary.LittleEndian.PutUint64(num[:], v)
		h.Write(num[:])
	}
	putBool := func(b bool) {
		var v byte
		if b {
			v = 1
		}
		h.Write([]byte{v})
	}

	oids := make([]uint32, 0, len(byOID))
	for oid := range byOID {
		oids = append(oids, oid)
	}
	sort.Slice(oids, func(i, j int) bool { return oids[i] < oids[j] })

	putUint(uint64(len(oids)))
	for _, oid := range oids {
		rel := byOID[oid]
		putUint(uint64(oid))
		put(rel.Schema)
		put(rel.Name)
		putBool(rel.RLSEnabled)
		putUint(uint64(len(rel.Policies)))
		for _, p := range rel.Policies {
			put(p.Name)
			putBool(p.Permissive)
			putUint(uint64(len(p.Roles)))
			for _, r := range p.Roles {
				put(r)
			}
			put(p.Using)
		}
	}

	putSet := func(set map[string]bool) {
		keys := make([]string, 0, len(set))
		for k := range set {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		putUint(uint64(len(keys)))
		for _, k := range keys {
			put(k)
		}
	}
	putSet(bypass)

	members := make([]string, 0, len(memberOf))
	for r := range memberOf {
		members = append(members, r)
	}
	sort.Strings(members)
	putUint(uint64(len(members)))
	for _, r := range members {
		put(r)
		putSet(memberOf[r])
	}

	var out [32]byte
	h.Sum(out[:0])
	return out
}

func (c *Cache) loadMemberships(ctx context.Context, q querier) (map[string]map[string]bool, error) {
	out := map[string]map[string]bool{}
	if len(c.roles) == 0 {
		return out, nil
	}
	rows, err := q.Query(ctx, memberQuery, c.roles)
	if err != nil {
		return nil, fmt.Errorf("catalog: query role memberships: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var role, of string
		if err := rows.Scan(&role, &of); err != nil {
			return nil, fmt.Errorf("catalog: scan role membership: %w", err)
		}
		if out[role] == nil {
			out[role] = map[string]bool{}
		}
		out[role][of] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: iterate role memberships: %w", err)
	}
	return out, nil
}

// MemberOf returns the roles whose privileges role has. The map is shared and
// must not be modified.
func (c *Cache) MemberOf(role string) map[string]bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.memberOf[role]
}

func (c *Cache) loadBypassRoles(ctx context.Context, q querier) (map[string]bool, error) {
	rows, err := q.Query(ctx, bypassQuery)
	if err != nil {
		return nil, fmt.Errorf("catalog: query bypassrls roles: %w", err)
	}
	defer rows.Close()

	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("catalog: scan bypassrls role: %w", err)
		}
		out[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: iterate bypassrls roles: %w", err)
	}
	// A failed refresh must never widen access, so the caller keeps the previous
	// snapshot on error rather than falling back to an empty -- or stale-open --
	// set. An empty set here is the safe direction: every role gets its policies
	// evaluated.
	return out, nil
}

// BypassesRLS reports whether RLS is skipped entirely for this role, because it
// holds BYPASSRLS or is a superuser.
//
// Unknown roles, and a cache that has not loaded yet, answer false: the only
// safe default is to evaluate the policies.
func (c *Cache) BypassesRLS(role string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.bypass[role]
}

// HasColumnPrivilege checks SELECT access for a role on specific columns.
//
// Column-level grants are checked separately from RLS and always: a column the
// role cannot select is never emitted, in any tier.
func (c *Cache) HasColumnPrivilege(ctx context.Context, role string, rel *Relation, columns []string) (map[string]bool, error) {
	return c.columnPrivileges(ctx, role, rel, columns, func(ctx context.Context, missing []string) (pgx.Rows, error) {
		return c.pool.Query(ctx,
			`SELECT col, has_column_privilege($1::regrole, $2::regclass, col, 'SELECT')
			   FROM unnest($3::text[]) AS col`,
			role, QuoteQualified(rel.Schema, rel.Name), missing)
	})
}

// HasColumnPrivilegeCurrent is HasColumnPrivilege for the pool's current role.
// Issuer snapshots and projections use physical SELECT, not the JWT role's ACL.
func (c *Cache) HasColumnPrivilegeCurrent(ctx context.Context, rel *Relation, columns []string) (map[string]bool, error) {
	return c.columnPrivileges(ctx, "", rel, columns, func(ctx context.Context, missing []string) (pgx.Rows, error) {
		return c.pool.Query(ctx,
			`SELECT col, has_column_privilege($1::regclass, col, 'SELECT')
			   FROM unnest($2::text[]) AS col`,
			QuoteQualified(rel.Schema, rel.Name), missing)
	})
}

// privilegeKey names one cached answer. An empty role is the pool's own.
type privilegeKey struct {
	role   string
	oid    uint32
	column string
}

// columnPrivileges answers from the cache and queries only the columns it does
// not hold. Answers are kept until the next Refresh, so a GRANT or REVOKE
// reaches new subscriptions within SLUICE_CATALOG_REFRESH, and a reconnect wave
// costs one query per relation and role instead of one per subscription.
func (c *Cache) columnPrivileges(
	ctx context.Context, role string, rel *Relation, columns []string,
	query func(ctx context.Context, missing []string) (pgx.Rows, error),
) (map[string]bool, error) {
	out := make(map[string]bool, len(columns))
	var missing []string
	c.privMu.Lock()
	gen := c.privGen
	for _, col := range columns {
		if ok, hit := c.privileges[privilegeKey{role, rel.OID, col}]; hit {
			out[col] = ok
		} else {
			missing = append(missing, col)
		}
	}
	c.privMu.Unlock()
	if len(missing) == 0 {
		return out, nil
	}

	rows, err := query(ctx, missing)
	if err != nil {
		return nil, fmt.Errorf("catalog: has_column_privilege: %w", err)
	}
	defer rows.Close()
	got := make(map[string]bool, len(missing))
	for rows.Next() {
		var col string
		var ok bool
		if err := rows.Scan(&col, &ok); err != nil {
			return nil, err
		}
		got[col] = ok
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	c.privMu.Lock()
	// An answer read before a refresh cleared the cache may predate a GRANT or
	// REVOKE the refresh saw, so it is used but not kept.
	keep := gen == c.privGen
	if keep && c.privileges == nil {
		c.privileges = map[privilegeKey]bool{}
	}
	for _, col := range missing {
		out[col] = got[col]
		if keep {
			c.privileges[privilegeKey{role, rel.OID, col}] = got[col]
		}
	}
	c.privMu.Unlock()
	return out, nil
}

// LoadedAt reports when the cache was last refreshed.
func (c *Cache) LoadedAt() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.loaded
}

// QuoteIdent quotes an identifier for use in dynamic SQL. Used only for
// relation and role names that have already been validated against the catalog.
func QuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// QuoteQualified quotes a schema-qualified name.
func QuoteQualified(schema, name string) string {
	return QuoteIdent(schema) + "." + QuoteIdent(name)
}
