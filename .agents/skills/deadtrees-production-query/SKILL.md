---
name: deadtrees-production-query
description: Query DeadTrees production records, ownership, audits and processing diagnostics directly with Python psycopg and the trusted analyst login.
---

# Query production

Use the provisioned `team_analyst` login, inheriting `analyst`. Retrieve
`DEADTREES_ANALYST_DATABASE_URL` through the operator's configured credential
store or ignored environment file. Never print the URI. Missing credentials
require access setup, not fallback to an administrator.

Install the small client dependency set in this checkout's Python environment:
`venv/bin/python -m pip install -r scripts/requirements-analyst.txt`.
For permissions and provisioning, read
[the access playbook](../../../docs/playbooks/analyst-database-access.md).

Use direct `psycopg` with `prepare_threshold=None` for transaction pooling.
Explicitly start a read-only transaction, set short timeouts, and verify the
identity before reading data. Role defaults alone are insufficient: inherited
PUBLIC extension privileges include HTTP-queue writes.

```python
import os
import psycopg

# The runtime supplies this variable, for example from a 1Password .env mount.
with psycopg.connect(os.environ['DEADTREES_ANALYST_DATABASE_URL'],
                     prepare_threshold=None, connect_timeout=10,
                     autocommit=True) as db:
    db.execute('BEGIN READ ONLY')
    db.execute("SET LOCAL statement_timeout = '30s'")
    db.execute("SET LOCAL lock_timeout = '3s'")
    identity = db.execute("SELECT current_user, session_user, "
        "pg_has_role(current_user, 'analyst', 'MEMBER'), "
        "current_setting('transaction_read_only')").fetchone()
    if identity != ('team_analyst', 'team_analyst', True, 'on'):
        raise RuntimeError('Unexpected analyst identity or transaction mode')
    rows = db.execute('SELECT id, file_name FROM public.v2_datasets '
                      'ORDER BY id DESC LIMIT 5').fetchall()
    db.execute('ROLLBACK')
```

Start with `v2_datasets`; outputs, statuses and audits join on `dataset_id`.
Queue and logs can have multiple rows per dataset. `user_info."user"` and
`analyst_access.accounts.id` link to dataset `user_id`; accounts exposes only
ID and email. Notification events link by `dataset_id`, `queue_task_id` and
`recipient_user_id`. Group one-to-many relations before combining counts.

Inspect the live catalog or migrations for other columns. Use base tables;
JWT-scoped API views are not part of this access. Filter and aggregate in SQL,
parameterize values, limit details, and avoid dumping personal data or raw logs.
Report the relevant scope and result; denied or inaccessible data is unknown,
not zero. Never test write denials against production.
