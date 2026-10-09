-- Rooms: a generic chat schema for the production-shaped profiles (rooms,
-- bigtxn, reconnect, calibrate), served by an issuer-mode Sluice with RLS off.
--
--   room_messages   one row per message; shapes pin room_id
--   room_members    one row per member; shapes pin user_id, and a member's
--                   row is the issuer hold of every shape it grants
--   notifications   one row per recipient; shapes pin user_id (bigtxn)
--
-- Every message is one transaction: one INSERT into room_messages and one
-- UPDATE of last_message_at on every member of the room. Most of those UPDATEs
-- reach no open stream; Sluice decodes and routes them all the same.
--
-- room_members starts with REPLICA IDENTITY FULL. The runner switches it to
-- the primary key (DEFAULT) or back with LOAD_REPLICA_IDENTITY before a run;
-- either way the hold filter (room_id, user_id) is in the replica identity.
--
-- Runs once on a fresh volume (docker-entrypoint-initdb.d).

\set ON_ERROR_STOP on

BEGIN;

CREATE TABLE public.room_messages (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  room_id    bigint      NOT NULL,
  seq        bigint      NOT NULL,
  author_id  uuid        NOT NULL,
  body       text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX room_messages_room_id ON public.room_messages (room_id);

CREATE TABLE public.room_members (
  room_id         bigint      NOT NULL,
  user_id         uuid        NOT NULL,
  last_message_at timestamptz,
  PRIMARY KEY (room_id, user_id)
);

CREATE INDEX room_members_user_id ON public.room_members (user_id);
ALTER TABLE public.room_members REPLICA IDENTITY FULL;

CREATE TABLE public.notifications (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  user_id    uuid        NOT NULL,
  kind       text        NOT NULL,
  body       text        NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX notifications_user_id ON public.notifications (user_id);

GRANT SELECT ON public.room_messages, public.room_members, public.notifications TO sluice_authz;
GRANT ALL    ON public.room_messages, public.room_members, public.notifications TO service_role;

CREATE PUBLICATION sluice_rooms FOR TABLE
  public.room_messages, public.room_members, public.notifications;

COMMIT;
