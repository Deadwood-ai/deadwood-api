"""Retry contracts through the real API, local database and storage."""

from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from uuid import uuid4
from zipfile import ZipFile

import pytest
import httpx
import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from fastapi.testclient import TestClient

from api.src.server import app
from shared.db import login, use_client, verify_token
from shared.settings import settings


@pytest.fixture
def upload(auth_token, data_directory):
	upload_id = f'retry-{uuid4()}'

	def send(index, content, total=3, filename='retry.tif', request_token=auth_token, **metadata):
		with TestClient(app) as client:
			return client.post(
				'/datasets/chunk',
				files={'file': (filename, content, 'application/octet-stream')},
				data={
					'upload_id': upload_id,
					'chunk_index': index,
					'chunks_total': total,
					'license': 'CC BY',
					'platform': 'drone',
					'authors': ['Retry test'],
					**metadata,
				},
				headers={'Authorization': f'Bearer {request_token}'},
			)

	return send, upload_id


def test_lost_middle_response_does_not_append_twice(upload):
	send, upload_id = upload
	assert send(0, b'first').status_code == 200
	assert send(1, b'second').status_code == 200  # response lost to the caller
	assert send(1, b'second').status_code == 200
	assert (settings.archive_path / f'{upload_id}.tmp').read_bytes() == b'firstsecond'


@pytest.mark.parametrize('filename', ['retry.tif', 'retry.zip'])
def test_retries_preserve_bytes_and_replay_final_dataset(upload, auth_token, filename):
	send, _ = upload
	if filename.endswith('.zip'):
		buffer = BytesIO()
		with ZipFile(buffer, 'w') as archive:
			archive.writestr('image.jpg', b'image fixture')
		content = buffer.getvalue()
	else:
		with MemoryFile() as image:
			with image.open(
				driver='GTiff',
				width=2,
				height=2,
				count=1,
				dtype='uint8',
				crs='EPSG:4326',
				transform=from_origin(7.8, 48.0, 0.001, 0.001),
			) as raster:
				raster.write(np.ones((1, 2, 2), dtype='uint8'))
			content = image.read()
	with use_client(auth_token) as db:
		before = {
			row['id'] for row in db.table(settings.datasets_table).select('id').eq('file_name', filename).execute().data
		}
	chunks = [content[:20], content[20:40], content[40:]]
	for index in [0, 1, 0, 1, 2]:
		response = send(index, chunks[index], filename=filename)
		assert response.status_code == 200, response.text
	first_result = response.json()
	for _ in range(2):
		retry = send(2, chunks[2], filename=filename)
		assert retry.status_code == 200, retry.text
		assert retry.json() == first_result
	dataset_id = first_result['id']
	path = (
		settings.raw_images_path / f'{dataset_id}.zip'
		if filename.endswith('.zip')
		else settings.archive_path / f'{dataset_id}_ortho.tif'
	)
	assert path.read_bytes() == content
	with use_client(auth_token) as db:
		after = {
			row['id'] for row in db.table(settings.datasets_table).select('id').eq('file_name', filename).execute().data
		}
		assert after - before == {dataset_id}
		status = db.table(settings.statuses_table).select('*').eq('dataset_id', dataset_id).single().execute().data
		assert status['is_upload_done'] is True
		assert db.table(settings.orthos_table).select('*').eq('dataset_id', dataset_id).execute().data == []
		if filename.endswith('.zip'):
			assert len(db.table(settings.raw_images_table).select('*').eq('dataset_id', dataset_id).execute().data) == 1


def test_rejects_missing_changed_and_out_of_order_chunks(upload):
	send, upload_id = upload
	assert send(1, b'second').status_code == 409
	assert send(0, b'first').status_code == 200
	assert send(2, b'last').status_code == 409
	assert send(0, b'changed').status_code == 409
	assert send(1, b'second', total=4).status_code == 409
	assert send(1, b'second', authors=['Different author']).status_code == 409
	assert (settings.archive_path / f'{upload_id}.tmp').read_bytes() == b'first'


def test_concurrent_final_requests_have_one_result(upload):
	send, _ = upload
	assert send(0, b'first', total=2).status_code == 200
	with ThreadPoolExecutor(max_workers=2) as pool:
		responses = list(pool.map(lambda _: send(1, b'last', total=2), range(2)))
	assert all(response.status_code in (200, 503) for response in responses)
	successes = [response.json() for response in responses if response.status_code == 200]
	assert successes
	retry = send(1, b'last', total=2)
	assert retry.status_code == 200
	assert all(result == retry.json() for result in successes)


@pytest.mark.parametrize(('index', 'total'), [(-1, 3), (0, 0), (3, 3)])
def test_invalid_chunk_numbers(upload, index, total):
	send, _ = upload
	assert send(index, b'chunk', total=total).status_code == 422


@pytest.mark.parametrize('upload_id', ['../escape', '/absolute', 'x' * 129, 'white space'])
def test_invalid_upload_id_is_rejected(upload, upload_id):
	send, _ = upload
	assert send(0, b'chunk', upload_id=upload_id).status_code == 422


def test_empty_chunk_is_rejected(upload):
	send, _ = upload
	assert send(0, b'').status_code == 422


def test_upload_id_cannot_be_taken_over_by_another_user(upload, test_user2):
	send, upload_id = upload
	assert send(0, b'first').status_code == 200
	other_token = login(settings.TEST_USER_EMAIL2, settings.TEST_USER_PASSWORD2, use_cached_session=False)
	assert verify_token(other_token).id == test_user2
	assert send(0, b'first', request_token=other_token).status_code == 409
	assert (settings.archive_path / f'{upload_id}.tmp').read_bytes() == b'first'


def test_refreshed_login_can_replay_single_chunk_upload(upload):
	send, _ = upload
	first = send(0, b'file bytes', total=1)
	assert first.status_code == 200
	new_token = login(settings.TEST_USER_EMAIL, settings.TEST_USER_PASSWORD, use_cached_session=False)
	retry = send(0, b'file bytes', total=1, request_token=new_token)
	assert retry.status_code == 200
	assert retry.json() == first.json()


def test_lost_database_insert_response_does_not_create_another_dataset(upload, auth_token, monkeypatch):
	send, upload_id = upload
	filename = f'{upload_id}.tif'
	original_send = httpx.Client.send
	insert_requests = []

	def lose_insert_response(http_client, request, *args, **kwargs):
		response = original_send(http_client, request, *args, **kwargs)
		if request.method == 'POST' and request.url.path == f'/rest/v1/{settings.datasets_table}':
			assert response.is_success
			insert_requests.append(request.url.path)
			raise httpx.ReadError('Simulated lost insert response', request=request)
		return response

	# Drop only the local DB's response, after the real insertion succeeded.
	monkeypatch.setattr(httpx.Client, 'send', lose_insert_response)
	first = send(0, b'upload bytes', total=1, filename=filename)
	assert first.status_code in (400, 500)
	retry = send(0, b'upload bytes', total=1, filename=filename)
	assert retry.status_code == 409
	assert 'finalization was interrupted' in retry.json()['detail']
	assert len(insert_requests) == 1
	with use_client(auth_token) as db:
		rows = db.table(settings.datasets_table).select('id').eq('file_name', filename).execute().data
		assert len(rows) == 1
	assert (settings.archive_path / f'{upload_id}.tmp').read_bytes() == b'upload bytes'
