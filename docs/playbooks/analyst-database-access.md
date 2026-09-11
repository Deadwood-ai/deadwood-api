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

## Routine production reads

Use direct Python `psycopg` with the provisioned `team_analyst` login for routine
production investigation. Do not use production PostgreSQL MCP or fall back to
an administrator. Existing purpose-specific monitor connections remain separate;
their metadata-only grants are described in
[operator data coverage](operator-data-coverage.md#dedicated-monitor-column-limited-reads).

The runtime must supply the complete connection URI as
`DEADTREES_ANALYST_DATABASE_URL` through your approved credential provider or
protected local environment. No particular password manager, personal helper,
mount path or colleague's machine is required. Obtain authorized access using
your team's credential process; missing credentials are an access gap. Preserve
application `.env` values, and never print the URI or put it in tracked files,
command arguments, logs or frontend env. The shared login does not provide
individual attribution or individual revocation.

Verify that the configured endpoint is the intended production target without
printing secrets. Use its actual pooler username convention, which can differ
from PostgreSQL's `current_user`. Install
[`scripts/requirements-analyst.txt`](../../scripts/requirements-analyst.txt) in
your checkout's Python environment:

```bash
venv/bin/python -m pip install -r scripts/requirements-analyst.txt
```

Run queries in an explicit read-only transaction with bounded timeouts and
transaction-pooler-compatible preparation disabled. Verify database, login,
analyst membership and read-only mode before reading application data:

```python
import os
import psycopg

# Your credential provider supplies the variable to this process.
with psycopg.connect(os.environ['DEADTREES_ANALYST_DATABASE_URL'],
                     prepare_threshold=None, connect_timeout=10,
                     autocommit=True) as db:
    db.execute('BEGIN READ ONLY')
    try:
        db.execute("SET LOCAL statement_timeout = '30s'")
        db.execute("SET LOCAL lock_timeout = '3s'")
        identity = db.execute(
            "SELECT current_database(), current_user, session_user, "
            "pg_has_role(current_user, 'analyst', 'MEMBER'), "
            "current_setting('transaction_read_only')"
        ).fetchone()
        if identity != ('postgres', 'team_analyst', 'team_analyst', True, 'on'):
            raise RuntimeError('Unexpected analyst target, identity or transaction mode')
        rows = db.execute('SELECT id FROM public.v2_datasets '
                          'ORDER BY id DESC LIMIT 5').fetchall()
    finally:
        db.execute('ROLLBACK')
```

Use base tables, explicit columns, filters, parameterized values and bounded
results. JWT-scoped API views are not part of this access. The
[query skill](../../.agents/skills/deadtrees-production-query/SKILL.md) provides
join guidance. Report inaccessible or denied data as unknown, not zero. A failed
identity check stops investigation; it does not authorize access or grant changes.
Never test write denials in production.

## Provisioning and permission validation

Provisioning is a separate, explicitly authorized administration operation.
Deploy the reviewed migration through the merge-to-main migration workflow.
Then an authorized administrator provisions a strong password and enables
LOGIN for `team_analyst` through the configured direct database administration
connection. Keep password values out of SQL files, arguments, logs and history;
supply them in memory through the credential store. Do not rotate other logins.
The shared identity has a 20-connection limit, 30-second statement timeout,
3-second lock timeout and `default_transaction_read_only=on`.

Verify production with catalog reads and representative queries in the explicit
read-only transaction above: identity/membership, private and archived datasets,
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
