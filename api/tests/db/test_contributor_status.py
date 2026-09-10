"""Owner status visibility and disclosure contracts on isolated PostgreSQL."""
import json
import uuid
from urllib.parse import urlparse

import psycopg
import pytest

from shared.settings import settings


@pytest.fixture
def status_db():
	assert urlparse(settings.SUPABASE_DB_URL).hostname in {'localhost', '127.0.0.1', 'host.docker.internal'}, 'Local database required'
	with psycopg.connect(settings.SUPABASE_DB_URL, user='supabase_admin') as db:
		try:
			yield db
		finally:
			db.rollback()


@pytest.mark.parametrize('archived', [False, True])
@pytest.mark.parametrize('access', ['private', 'public'])
def test_excluded_owner_can_read_review_but_other_users_cannot(status_db, archived, access):
	db = status_db
	owner, other = uuid.uuid4(), uuid.uuid4()
	for user in (owner, other):
		db.execute("INSERT INTO auth.users(id,email) VALUES (%s,%s)", (user, f'{user}@example.invalid'))
	dataset = db.execute(
		"INSERT INTO public.v2_datasets(user_id,file_name,license,platform,data_access,archived) VALUES (%s,'excluded.tif','CC BY','drone',%s,%s) RETURNING id",
		(owner, access, archived),
	).fetchone()[0]
	db.execute("INSERT INTO public.v2_statuses(dataset_id,has_error,error_message,error_stage) VALUES (%s,true,'PRIVATE DIAGNOSTIC','cog_processing')", (dataset,))
	db.execute(
		"INSERT INTO public.dataset_audit(dataset_id,audited_by,final_assessment,notes,has_cog_issue,cog_issue_notes) VALUES (%s,%s,'exclude_completely','Image is not georeferenced',true,'Map image has missing pixels')",
		(dataset, other),
	)
	db.execute('SET LOCAL ROLE authenticated')
	db.execute("SELECT set_config('request.jwt.claims',%s,true)", (json.dumps({'sub': str(owner), 'role': 'authenticated'}),))
	row = db.execute('SELECT to_jsonb(v) FROM public.v2_full_dataset_view_owner v WHERE id=%s', (dataset,)).fetchone()[0]
	assert row['archived'] is archived
	assert row['final_assessment'] == 'exclude_completely'
	assert row['error_stage'] == 'cog_processing'
	assert 'error_message' not in row
	assert 'PRIVATE DIAGNOSTIC' not in json.dumps(row)
	details = db.execute('SELECT public.get_dataset_status_details(%s)', (dataset,)).fetchone()[0]
	assert details['notes'] == 'Image is not georeferenced'
	assert details['cog_issue_notes'] == 'Map image has missing pixels'
	assert not {'audited_by', 'audited_by_email', 'uploaded_by_email'} & details.keys()
	db.execute("SELECT set_config('request.jwt.claims',%s,true)", (json.dumps({'sub': str(other), 'role': 'authenticated'}),))
	assert db.execute('SELECT id FROM public.v2_full_dataset_view_owner WHERE id=%s', (dataset,)).fetchall() == []
	assert db.execute('SELECT public.get_dataset_status_details(%s)', (dataset,)).fetchone()[0] is None
	assert db.execute('SELECT id FROM public.v2_full_dataset_view_public WHERE id=%s', (dataset,)).fetchall() == []
	db.execute('SET LOCAL ROLE anon')
	assert db.execute('SELECT id FROM public.v2_full_dataset_view_public WHERE id=%s', (dataset,)).fetchall() == []
	with pytest.raises(psycopg.errors.InsufficientPrivilege):
		db.execute('SELECT public.get_dataset_status_details(%s)', (dataset,))
