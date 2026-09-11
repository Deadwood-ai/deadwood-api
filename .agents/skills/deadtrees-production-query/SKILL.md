---
name: deadtrees-production-query
description: Query DeadTrees production records, ownership, audits and processing diagnostics directly with Python psycopg and the trusted analyst login.
---

# Query production

Follow the canonical
[routine production read procedure](../../../docs/playbooks/analyst-database-access.md#routine-production-reads)
for credential loading, dependency installation, the direct psycopg example and
required target/identity/read-only checks. Use the provisioned `team_analyst`
login; missing access does not authorize production PostgreSQL MCP or an
administrator fallback. Keep every query inside that verified transaction.

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
