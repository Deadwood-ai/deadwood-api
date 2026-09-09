"""Filesystem recovery and process locking without external services."""

from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context

import pytest
from fastapi import HTTPException

from api.src.upload.chunk_session import locked_chunk_session


pytestmark = pytest.mark.unit
CONTRACT = {'chunks_total': 2, 'filename': 'image.tif'}


def open_session(tmp_path, user='owner', contract=None):
	return locked_chunk_session(tmp_path / 'sessions', 'upload-id', tmp_path / 'upload.tmp', user, contract or CONTRACT)


def attempt_in_another_process(tmp_path):
	try:
		with open_session(tmp_path) as session:
			return session.accept(0, b'first')
	except HTTPException as error:
		return error.status_code


def test_worker_lock_is_shared_and_released(tmp_path):
	with ProcessPoolExecutor(max_workers=1, mp_context=get_context('spawn')) as pool:
		with open_session(tmp_path) as session:
			session.accept(0, b'first')
			assert pool.submit(attempt_in_another_process, tmp_path).result(timeout=20) == 503
		assert pool.submit(attempt_in_another_process, tmp_path).result(timeout=20) == {
			'message': 'Chunk 0 of 2 received'
		}
	assert (tmp_path / 'upload.tmp').read_bytes() == b'first'


@pytest.mark.parametrize('uncommitted', [b'par', b'last'])
def test_recovers_partial_or_complete_append_without_receipt(tmp_path, uncommitted):
	with open_session(tmp_path) as session:
		session.accept(0, b'first')
	with (tmp_path / 'upload.tmp').open('ab') as output:
		output.write(uncommitted)
	with open_session(tmp_path) as session:
		assert session.accept(1, b'last') is None
		session.begin_finalization()
		session.complete({'id': 123})
	assert (tmp_path / 'upload.tmp').read_bytes() == b'firstlast'


def test_lost_final_response_survives_new_session_and_file_move(tmp_path):
	with open_session(tmp_path) as session:
		session.accept(0, b'first')
		session.accept(1, b'last')
		session.begin_finalization()
		(tmp_path / 'upload.tmp').rename(tmp_path / '123_ortho.tif')
		session.complete({'id': 123})
	with open_session(tmp_path) as session:
		assert session.accept(1, b'last') == {'id': 123}
		with pytest.raises(HTTPException, match='Chunk content changed'):
			session.accept(1, b'other')
	assert not (tmp_path / 'upload.tmp').exists()


def test_interrupted_finalization_never_starts_again(tmp_path):
	with open_session(tmp_path) as session:
		session.accept(0, b'first')
		session.accept(1, b'last')
		session.begin_finalization()
		# The worker disappears after a DB insert or rename, before its receipt.
		(tmp_path / 'upload.tmp').rename(tmp_path / '123_ortho.tif')
	with open_session(tmp_path) as session:
		with pytest.raises(HTTPException, match='finalization was interrupted') as error:
			session.accept(1, b'last')
		assert error.value.status_code == 409
	assert not (tmp_path / 'upload.tmp').exists()


def test_owner_and_contract_are_bound_to_upload_id(tmp_path):
	with open_session(tmp_path) as session:
		session.accept(0, b'first')
	for user, contract in [('other', CONTRACT), ('owner', {**CONTRACT, 'filename': 'changed.zip'})]:
		with pytest.raises(HTTPException) as error:
			with open_session(tmp_path, user, contract):
				pytest.fail('Conflicting upload accepted')
		assert error.value.status_code == 409
	assert (tmp_path / 'upload.tmp').read_bytes() == b'first'


def test_legacy_untracked_bytes_are_not_overwritten(tmp_path):
	(tmp_path / 'upload.tmp').write_bytes(b'legacy upload')
	with pytest.raises(HTTPException, match='no retry receipts'):
		with open_session(tmp_path):
			pytest.fail('Legacy upload accepted')
	assert (tmp_path / 'upload.tmp').read_bytes() == b'legacy upload'


def test_missing_committed_bytes_prevent_finalization(tmp_path):
	with open_session(tmp_path) as session:
		session.accept(0, b'first')
		session.accept(1, b'last')
	(tmp_path / 'upload.tmp').write_bytes(b'first')
	with open_session(tmp_path) as session:
		assert session.accept(1, b'last') is None
		with pytest.raises(HTTPException, match='data is missing'):
			session.begin_finalization()
