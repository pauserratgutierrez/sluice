package catalog

import (
	"testing"

	"github.com/pauserratgutierrez/sluice/internal/expr"
)

func rlsRelation(policies ...Policy) *Relation {
	return &Relation{
		Schema: "public", Name: "posts",
		RLSEnabled: true,
		Columns: []Column{
			{Name: "id", TypeName: "bigint", AttNum: 1, NotNull: true},
			{Name: "owner_id", TypeName: "uuid", AttNum: 2, NotNull: true},
		},
		Policies: policies,
	}
}

func policy(t *testing.T, name string, permissive bool, roles []string, using string) Policy {
	t.Helper()
	node, err := expr.Parse(using)
	if err != nil {
		t.Fatalf("parse %q: %v", using, err)
	}
	return Policy{Name: name, Permissive: permissive, Roles: roles, Using: using, Parsed: node}
}

// A role holding BYPASSRLS sees every row, exactly as PostgreSQL's
// check_enable_rls() decides it. Before this was modeled, service_role was
// denied outright on any table whose policies did not happen to name it, which
// contradicted what the same JWT gets from PostgREST.
func TestPredicateBypassRLS(t *testing.T) {
	rel := rlsRelation(policy(t, "owner_reads", true, []string{"authenticated"},
		"owner_id = current_setting('request.jwt.claims')"))

	t.Run("bypass role sees everything", func(t *testing.T) {
		node, reason, sql := rel.Predicate("service_role", true)
		if !expr.IsAlwaysTrue(node) {
			t.Fatalf("BYPASSRLS role got a restricting predicate: %#v", node)
		}
		if reason != "" {
			t.Errorf("unexpected parse issue: %q", reason)
		}
		if sql != "true" {
			t.Errorf("PredicateSQL = %q, want %q", sql, "true")
		}
	})

	// The same role without the attribute must still be default-denied: the
	// bypass has to come from the catalog, never from the role's name.
	t.Run("same role without the attribute is denied", func(t *testing.T) {
		node, _, sql := rel.Predicate("service_role", false)
		if !expr.IsAlwaysFalse(node) {
			t.Fatalf("role with no applicable policy was not denied: %#v", node)
		}
		if sql != "false" {
			t.Errorf("PredicateSQL = %q, want %q", sql, "false")
		}
	})

	t.Run("non-bypass role still gets its policy", func(t *testing.T) {
		node, _, sql := rel.Predicate("authenticated", false)
		if expr.IsAlwaysTrue(node) || expr.IsAlwaysFalse(node) {
			t.Fatalf("policy predicate collapsed to a constant: %#v", node)
		}
		if sql == "true" {
			t.Errorf("PredicateSQL unexpectedly unrestricted: %q", sql)
		}
	})
}

// A restrictive policy must not survive the bypass either: PostgreSQL skips the
// whole row-security system for these roles, restrictive policies included.
func TestPredicateBypassIgnoresRestrictivePolicy(t *testing.T) {
	rel := rlsRelation(
		policy(t, "all_read", true, nil, "true"),
		policy(t, "not_archived", false, nil, "archived = false"),
	)
	if node, _, _ := rel.Predicate("postgres", true); !expr.IsAlwaysTrue(node) {
		t.Fatalf("restrictive policy survived BYPASSRLS: %#v", node)
	}
	if node, _, _ := rel.Predicate("authenticated", false); expr.IsAlwaysTrue(node) {
		t.Fatal("restrictive policy was dropped for a non-bypass role")
	}
}

// The fingerprint is what makes a revocation take effect on a decision that
// carries no lease. Anything Predicate reads must move it; anything it does not
// read must not, or every refresh re-resolves every subscription for nothing.
func TestAuthzFingerprint(t *testing.T) {
	base := func() (map[uint32]*Relation, map[string]bool) {
		rel := rlsRelation(
			policy(t, "owner_reads", true, []string{"authenticated"}, "owner_id = current_setting('x')"),
			policy(t, "not_archived", false, nil, "archived = false"),
		)
		rel.OID = 42
		rel.sortPolicies() // as Refresh leaves them
		return map[uint32]*Relation{42: rel}, map[string]bool{"service_role": true}
	}

	rels, bypass := base()
	want := authzFingerprint(rels, bypass)

	t.Run("stable across identical loads", func(t *testing.T) {
		rels2, bypass2 := base()
		if got := authzFingerprint(rels2, bypass2); got != want {
			t.Error("fingerprint changed without any input changing")
		}
	})

	// pg_policy rows arrive in no particular order. If that leaked into the
	// fingerprint, every refresh would look like a policy change and re-resolve
	// every subscription -- the failure mode that makes people disable the check.
	t.Run("row order does not move it", func(t *testing.T) {
		rels2, bypass2 := base()
		p := rels2[42].Policies
		p[0], p[1] = p[1], p[0]
		rels2[42].sortPolicies()
		if got := authzFingerprint(rels2, bypass2); got != want {
			t.Error("fingerprint depends on the order policies were loaded in")
		}
	})

	// Each of these is a revocation or a grant that must reach a live stream.
	for _, tc := range []struct {
		name   string
		mutate func(map[uint32]*Relation, map[string]bool)
	}{
		{"policy dropped", func(r map[uint32]*Relation, _ map[string]bool) {
			r[42].Policies = r[42].Policies[:1]
		}},
		{"policy expression rewritten", func(r map[uint32]*Relation, _ map[string]bool) {
			r[42].Policies[0].Using = "true"
		}},
		{"policy roles narrowed", func(r map[uint32]*Relation, _ map[string]bool) {
			r[42].Policies[0].Roles = []string{"anon"}
		}},
		{"policy flipped to restrictive", func(r map[uint32]*Relation, _ map[string]bool) {
			r[42].Policies[0].Permissive = !r[42].Policies[0].Permissive
		}},
		{"rls enabled on a table", func(r map[uint32]*Relation, _ map[string]bool) {
			r[42].RLSEnabled = false
		}},
		{"relation left the publication", func(r map[uint32]*Relation, _ map[string]bool) {
			delete(r, 42)
		}},
		{"bypassrls revoked from a role", func(_ map[uint32]*Relation, b map[string]bool) {
			delete(b, "service_role")
		}},
		{"bypassrls granted to a role", func(_ map[uint32]*Relation, b map[string]bool) {
			b["anon"] = true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rels2, bypass2 := base()
			tc.mutate(rels2, bypass2)
			if got := authzFingerprint(rels2, bypass2); got == want {
				t.Errorf("%s did not move the fingerprint, so a live subscription would never be re-resolved", tc.name)
			}
		})
	}

	// Length prefixing: neighbouring fields must not be able to borrow each
	// other's characters and hash the same.
	// Two adjacent role names with the same concatenation must not hash alike.
	// Whoever can name a policy or a role is a DBA, so this is hygiene rather
	// than a live attack, but a fingerprint that can be made to collide is a
	// revocation that can be made not to happen.
	t.Run("field boundaries are unambiguous", func(t *testing.T) {
		a, ab := base()
		a[42].Policies[0].Roles = []string{"ab", "c"}
		b, bb := base()
		b[42].Policies[0].Roles = []string{"a", "bc"}
		if authzFingerprint(a, ab) == authzFingerprint(b, bb) {
			t.Error("adjacent fields collided; the hash is not length-prefixed")
		}
	})
}

func TestBypassesRLSDefaultsClosed(t *testing.T) {
	c := New(nil)
	if c.BypassesRLS("service_role") {
		t.Error("unloaded cache reported a bypass; it must default closed")
	}
	c.bypass = map[string]bool{"service_role": true}
	if !c.BypassesRLS("service_role") {
		t.Error("loaded bypass role not reported")
	}
	if c.BypassesRLS("authenticated") {
		t.Error("role outside the bypass set reported as bypassing")
	}
}
