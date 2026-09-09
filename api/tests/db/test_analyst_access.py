"""Analyst contract against isolated PostgreSQL; never run against production."""

import secrets
import uuid

import psycopg
from psycopg import sql
import pytest

from shared.settings import settings


@pytest.fixture
def db():
	with psycopg.connect(settings.SUPABASE_DB_URL, user='supabase_admin') as connection:
		try:
			yield connection
		finally:
			connection.rollback()


def become_analyst(db):
	# Session identity is necessary: SET ROLE alone still lets the admin switch roles.
	db.execute('SET SESSION AUTHORIZATION team_analyst')


def test_private_archived_ownership_audit_and_notification_reads(db):
	owner = uuid.uuid4()
	db.execute(
		"INSERT INTO auth.users (id,email,encrypted_password) VALUES (%s,'analyst@example.invalid','excluded')",
		(owner,),
	)
	dataset = db.execute(
		'INSERT INTO public.v2_datasets (user_id,file_name,license,platform,data_access,archived) '
		"VALUES (%s,'analyst.tif','CC BY','drone','private',true) RETURNING id",
		(owner,),
	).fetchone()[0]
	db.execute(
		"INSERT INTO public.dataset_audit (dataset_id,notes,audited_by) VALUES (%s,'Review detail',%s)",
		(dataset, owner),
	)
	db.execute(
		'INSERT INTO public.processing_notification_events '
		'(queue_task_id,dataset_id,event_type,recipient_user_id,recipient_email,delivery_error,provider_message_id) '
		"VALUES (987654321,%s,'processing_failed',%s,'analyst@example.invalid','Provider rejected','test-message')",
		(dataset, owner),
	)
	become_analyst(db)
	assert db.execute(
		'SELECT d.user_id,a.email,d.archived FROM public.v2_datasets d '
		'JOIN analyst_access.accounts a ON a.id=d.user_id WHERE d.id=%s',
		(dataset,),
	).fetchone() == (owner, 'analyst@example.invalid', True)
	assert db.execute(
		'SELECT notes,audited_by FROM public.dataset_audit WHERE dataset_id=%s', (dataset,)
	).fetchone() == ('Review detail', owner)
	assert db.execute(
		'SELECT recipient_email,delivery_error,provider_message_id FROM public.processing_notification_events WHERE dataset_id=%s',
		(dataset,),
	).fetchone() == ('analyst@example.invalid', 'Provider rejected', 'test-message')
	# Forging application claims cannot add privileges to a direct SQL identity.
	db.execute(
		"SELECT set_config('request.jwt.claims', %s, true)",
		('{"role":"service_role","email":"processor@deadtrees.earth"}',),
	)
	with pytest.raises(psycopg.errors.InsufficientPrivilege):
		db.execute('UPDATE public.v2_datasets SET archived=false WHERE id=%s', (dataset,))


@pytest.mark.parametrize(
	'query',
	[
		'SELECT encrypted_password FROM auth.users',
		'SELECT * FROM auth.sessions',
		'SELECT token_hash FROM public.prepackaged_dataset_download_grants',
		'SELECT extra FROM public.prepackaged_dataset_download_grants',
		'SELECT requested_ip FROM public.prepackaged_dataset_download_grants',
		'SELECT * FROM public.newsletter',
		'SELECT * FROM vault.decrypted_secrets',
		'SELECT * FROM storage.objects',
		'SELECT * FROM public.v2_processing_overview',
		"INSERT INTO public.priwa_projects (slug,name) VALUES ('analyst-denied','Denied')",
		'UPDATE public.v2_datasets SET archived=true WHERE false',
		'DELETE FROM public.v2_queue WHERE false',
		'TRUNCATE public.processing_notification_events',
		"SELECT nextval('public.v2_datasets_id_seq')",
		'CREATE TABLE public.analyst_denied (id integer)',
		'CREATE SCHEMA analyst_denied',
		'CREATE ROLE analyst_denied',
		'ALTER ROLE team_analyst SUPERUSER',
		'GRANT analyst TO authenticated',
		'SET ROLE authenticated',
		'SET ROLE service_role',
		'SET ROLE postgres',
		'SELECT public.approve_correction(1, null)',
		"SELECT public.insert_tile_embeddings(1, '[]'::jsonb)",
	],
)
def test_forbidden_reads_writes_and_escalation(db, query):
	become_analyst(db)
	with pytest.raises(psycopg.errors.InsufficientPrivilege):
		db.execute(query)


def test_definer_trigger_cannot_be_attached_to_owned_table(db):
	become_analyst(db)
	db.execute('CREATE TEMP TABLE analyst_trigger_probe (id bigint)')
	with pytest.raises(psycopg.errors.InsufficientPrivilege):
		db.execute(
			'CREATE TRIGGER probe AFTER UPDATE ON analyst_trigger_probe FOR EACH ROW EXECUTE FUNCTION public.log_dataset_changes()'
		)


def test_role_inventory_reads_and_effective_privileges(db):
	for name in ('analyst', 'team_analyst'):
		assert (
			db.execute(
				'SELECT rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=%s',
				(name,),
			).fetchone()
			== (False,) * 6
		)
	assert db.execute("SELECT pg_has_role('team_analyst','analyst','MEMBER')").fetchone() == (True,)
	assert db.execute(
		"SELECT admin_option FROM pg_auth_members WHERE member='team_analyst'::regrole AND roleid='analyst'::regrole"
	).fetchone() == (False,)
	for role in ('anon', 'authenticated', 'service_role', 'authenticator', 'deadtrees_operator_status'):
		assert not db.execute("SELECT pg_has_role(%s,'analyst','MEMBER')", (role,)).fetchone()[0]
	settings = db.execute("SELECT rolconfig FROM pg_roles WHERE rolname='team_analyst'").fetchone()[0]
	assert {'default_transaction_read_only=on', 'statement_timeout=30s', 'lock_timeout=3s'} <= set(settings)
	tables = db.execute("SELECT tablename,cmd FROM pg_policies WHERE policyname='analyst_select'").fetchall()
	assert len(tables) == 54
	for table, command in tables:
		assert command == 'SELECT'
		for privilege in ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'TRIGGER', 'REFERENCES'):
			assert not db.execute(
				'SELECT has_table_privilege(%s,%s,%s)', ('team_analyst', 'public.' + table, privilege)
			).fetchone()[0]
		with db.transaction():
			db.execute('SET LOCAL ROLE team_analyst')
			columns = 'id' if table == 'prepackaged_dataset_download_grants' else '*'
			db.execute(
				sql.SQL('SELECT {} FROM public.{} LIMIT 1').format(sql.SQL(columns), sql.Identifier(table))
			).fetchall()
			db.execute('RESET ROLE')
	assert not db.execute(
		'SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace '
		"WHERE n.nspname='public' AND p.prosecdef AND has_function_privilege('team_analyst',p.oid,'EXECUTE') "
		"AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.deptype='e'))"
	).fetchone()[0]
	for schema in ('auth', 'storage', 'vault', 'internal'):
		assert not db.execute("SELECT has_schema_privilege(%s,%s,'USAGE')", ('team_analyst', schema)).fetchone()[0]
	assert db.execute(
		"SELECT array_agg(column_name::text ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema='analyst_access' AND table_name='accounts'"
	).fetchone()[0] == ['id', 'email']


def test_future_objects_do_not_inherit_analyst_access(db):
	db.execute('SET LOCAL ROLE postgres')
	db.execute('CREATE TABLE public.analyst_future_probe (id integer)')
	db.execute(
		'CREATE FUNCTION public.analyst_future_probe() RETURNS integer LANGUAGE sql SECURITY DEFINER AS $$ SELECT 1 $$'
	)
	db.execute('RESET ROLE')
	assert not db.execute(
		"SELECT has_table_privilege('team_analyst','public.analyst_future_probe','SELECT')"
	).fetchone()[0]
	assert not db.execute(
		"SELECT has_function_privilege('team_analyst','public.analyst_future_probe()','EXECUTE')"
	).fetchone()[0]


def test_read_only_transaction_blocks_inherited_extension_writes(db):
	# This known PUBLIC grant is deliberately preserved for extension/application callers.
	assert db.execute("SELECT has_table_privilege('team_analyst','net.http_request_queue','INSERT')").fetchone()[0]
	db.rollback()
	db.execute('BEGIN READ ONLY')
	become_analyst(db)
	assert db.execute("SELECT current_user,current_setting('transaction_read_only')").fetchone() == (
		'team_analyst',
		'on',
	)
	with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
		db.execute('DELETE FROM net.http_request_queue WHERE false')


def test_real_login_defaults_and_session_identity_cannot_escalate():
	password = secrets.token_urlsafe(32)
	with psycopg.connect(settings.SUPABASE_DB_URL, user='supabase_admin', autocommit=True) as admin:
		try:
			admin.execute(sql.SQL('ALTER ROLE team_analyst LOGIN PASSWORD {}').format(sql.Literal(password)))
			with psycopg.connect(
				settings.SUPABASE_DB_URL,
				user='team_analyst',
				password=password,
				prepare_threshold=None,
				autocommit=True,
			) as client:
				client.execute('BEGIN READ ONLY')
				assert client.execute(
					"SELECT current_user,session_user,current_setting('transaction_read_only')"
				).fetchone() == ('team_analyst', 'team_analyst', 'on')
				assert client.execute('SHOW statement_timeout').fetchone() == ('30s',)
				assert client.execute('SHOW lock_timeout').fetchone() == ('3s',)
				client.execute('SELECT id FROM public.v2_datasets LIMIT 1').fetchall()
				with pytest.raises(psycopg.errors.InsufficientPrivilege):
					client.execute('SET SESSION AUTHORIZATION postgres')
		finally:
			admin.execute('ALTER ROLE team_analyst NOLOGIN PASSWORD NULL')
