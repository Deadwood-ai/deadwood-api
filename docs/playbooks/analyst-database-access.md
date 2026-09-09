# Trusted analyst database access

`analyst` is a NOLOGIN permission role. `team_analyst` inherits it without the
admin option and is created NOLOGIN until separately provisioned. Neither role
has superuser, role/database creation, replication or RLS bypass privileges.
This is trusted internal access to private and archived records, including
personal information; it must not be exposed through PostgREST or JWT roles.

The migration explicitly grants SELECT on the current application table
inventory and adds SELECT-only RLS policies. It includes output metadata,
audit details, ownership and publication relationships, PRIWA records and
notification diagnostics. Download grants expose lifecycle and owner columns,
excluding token hashes, request fingerprints and the arbitrary `extra` payload.
Newsletter subscriptions are excluded. `analyst_access.accounts` exposes only
account ID and email through an owner-rights view; direct auth, storage, vault,
internal-schema and API-view access is not granted.

The role receives no application writes, sequence advancement or schema
creation. PUBLIC execution is removed from existing application SECURITY
DEFINER functions, including triggers, while preserving the existing API and
monitor roles' effective access. Future postgres-owned functions need explicit
execution grants. Future tables need explicit SELECT grants and RLS policies;
new sensitive columns on fully granted tables require an access review.

Extension-owned PUBLIC permissions remain unchanged. In particular, `pg_net`
allows every login to write HTTP queues outside read-only transactions. This
is not a database-wide read-only guarantee. Always use `BEGIN READ ONLY`,
short transaction-local timeouts, and a trusted client. Never grant analyst
membership to an application role or a login with other write privileges.

## Provision and connect

Deploy the reviewed migration through the merge-to-main migration workflow.
Then an authorized administrator provisions a strong password and enables
LOGIN for `team_analyst` through the configured direct database administration
connection. Keep password values out of SQL files, arguments, logs and history;
supply them in memory through the credential store. Do not rotate other logins.
The shared identity has a 20-connection limit, 30-second statement timeout,
3-second lock timeout and `default_transaction_read_only=on`.

Store the complete URI as concealed `DEADTREES_ANALYST_DATABASE_URL` in the
project's approved 1Password Developer Environment. Mount it in an ignored
operator file such as `.local/analyst.env`; preserve existing application `.env`
values. Use the configured endpoint and actual pooler username convention,
which can differ from PostgreSQL's `current_user`. Never put it in frontend env
files. Each colleague needs authorized access to that Environment; the shared
login does not provide individual attribution or individual revocation.

Install `scripts/requirements-analyst.txt` in the checkout's `venv`. Load only
the selected operator environment at runtime (for example with
`dotenv.load_dotenv('.local/analyst.env')`). The
[query skill](../../.agents/skills/deadtrees-production-query/SKILL.md) provides
the direct psycopg example, identity check and join guidance.

Verify production with catalog reads and representative queries in an explicit
read-only transaction: new identity/membership, private and archived datasets,
output metadata, audit detail, owner lookup and notification diagnostics.
Verify existing monitor access separately. Production write-denial probes are
not permitted by this verification procedure.

## Local validation

Use the isolated environment sequence in
[environment and access](../agents/environment-and-access.md), then run
`api/tests/db/test_analyst_access.py` and the existing monitor tests through the
API test container. The API smoke suite includes both. Tests cover real private
reads, excluded columns, writes, forged claims, role escalation, sequences,
definer triggers, future objects and read-only protection of inherited pg_net
writes. Run the full API smoke suite before delivery to check preserved callers.
