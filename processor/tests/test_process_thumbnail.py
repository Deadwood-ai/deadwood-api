import pytest
from pathlib import Path

from shared.settings import settings
from shared.supabase import use_client
from shared.models import StatusEnum
from processor.src.process_thumbnail import process_thumbnail
from processor.src.exceptions import AuthenticationError, DatasetError, ProcessingError


def test_process_thumbnail_success(test_task, auth_token, monkeypatch):
	"""Test successful thumbnail processing"""
	# Process the thumbnail
	process_thumbnail(test_task, settings.processing_path)

	# Check if thumbnail metadata was saved to database
	with use_client(auth_token) as client:
		response = client.table(settings.thumbnail_table).select('*').eq('dataset_id', test_task.dataset_id).execute()
		assert len(response.data) == 1
		assert response.data[0]['dataset_id'] == test_task.dataset_id

		# Clean up by removing the test entry
		client.table(settings.thumbnail_table).delete().eq('dataset_id', test_task.dataset_id).execute()
