import os
import sys
import types
from enum import Enum

import requests
import pytest
from urllib3.exceptions import ReadTimeoutError

os.environ.setdefault('SUPABASE_URL', 'http://localhost')
os.environ.setdefault('SUPABASE_KEY', 'test')


class _NoopLogger:
	def info(self, *_args, **_kwargs):
		pass

	def warning(self, *_args, **_kwargs):
		pass

	def error(self, *_args, **_kwargs):
		pass


class _TestLogCategory(Enum):
	TREECOVER = 'treecover'


class _TestLogContext:
	def __init__(self, **kwargs):
		self.__dict__.update(kwargs)


logger_module = types.ModuleType('shared.logger')
logger_module.logger = _NoopLogger()
sys.modules.setdefault('shared.logger', logger_module)

logging_module = types.ModuleType('shared.logging')
logging_module.LogCategory = _TestLogCategory
logging_module.LogContext = _TestLogContext
sys.modules.setdefault('shared.logging', logging_module)

db_module = types.ModuleType('shared.db')
db_module.login = lambda *_args, **_kwargs: 'token'
db_module.verify_token = lambda *_args, **_kwargs: True
sys.modules.setdefault('shared.db', db_module)

labels_module = types.ModuleType('shared.labels')
labels_module.create_label_with_geometries = lambda *_args, **_kwargs: None
labels_module.delete_model_prediction_labels = lambda *_args, **_kwargs: 0
sys.modules.setdefault('shared.labels', labels_module)

segmentation_module = types.ModuleType('processor.src.utils.segmentation')
segmentation_module.mask_to_polygons = lambda *_args, **_kwargs: []
segmentation_module.reproject_polygons = lambda polygons, *_args, **_kwargs: polygons
segmentation_module.filter_polygons_by_area = lambda polygons, *_args, **_kwargs: polygons
segmentation_module.get_utm_string_from_latlon = lambda *_args, **_kwargs: 'EPSG:32632'
sys.modules.setdefault('processor.src.utils.segmentation', segmentation_module)

from processor.src.treecover_segmentation_oam_tcd import predict_treecover  # noqa: E402

pytestmark = pytest.mark.unit


class _FakeImages:
	def get(self, _image):
		return object()


class _FakeContainer:
	def __init__(self):
		self.killed = False
		self.removed = False
		self.name = 'fake-tcd-container'
		self.wait_timeout = None

	def wait(self, timeout):
		self.wait_timeout = timeout
		raise requests.exceptions.ConnectionError(ReadTimeoutError(None, None, 'Read timed out.'))

	def kill(self):
		self.killed = True

	def remove(self, force=False):
		self.removed = force


class _FakeContainers:
	def __init__(self):
		self.runs = []

	def run(self, **_kwargs):
		container = _FakeContainer()
		self.runs.append(container)
		return container


class _FakeDockerClient:
	def __init__(self):
		self.images = _FakeImages()
		self.containers = _FakeContainers()


def test_tcd_wait_connection_timeout_is_controlled_and_not_retried(monkeypatch):
	client = _FakeDockerClient()
	bundles = []

	monkeypatch.setattr(predict_treecover.docker, 'from_env', lambda: client)
	monkeypatch.setattr(
		predict_treecover,
		'build_container_forensics',
		lambda *args, **kwargs: {'dataset_id': kwargs['dataset_id'], 'stage': kwargs['stage']},
	)
	monkeypatch.setattr(
		predict_treecover,
		'write_debug_bundle',
		lambda **kwargs: bundles.append(kwargs),
	)
	monkeypatch.setattr(predict_treecover.settings, 'TCD_CONTAINER_TIMEOUT_SECONDS', 14400)
	monkeypatch.setattr(predict_treecover.settings, 'TCD_CONTAINER_TIMEOUT_MAX_SECONDS', 43200)
	monkeypatch.setattr(predict_treecover.settings, 'TCD_CONTAINER_TIMEOUT_BASE_PIXELS', 2_000_000_000)

	with pytest.raises(
		predict_treecover._TCDContainerTimeout,
		match='TCD container timed out after 4 hours',
	):
		predict_treecover._run_tcd_pipeline_container('tcd_volume_10179_test', 10179, 'token')

	assert len(client.containers.runs) == 1
	assert client.containers.runs[0].killed is True
	assert client.containers.runs[0].removed is True
	assert client.containers.runs[0].wait_timeout == 14400
	assert bundles


def test_tcd_timeout_policy_uses_base_timeout_without_input_size(monkeypatch):
	monkeypatch.setattr(predict_treecover.settings, 'TCD_CONTAINER_TIMEOUT_SECONDS', 14400)
	monkeypatch.setattr(predict_treecover.settings, 'TCD_CONTAINER_TIMEOUT_MAX_SECONDS', 43200)
	monkeypatch.setattr(predict_treecover.settings, 'TCD_CONTAINER_TIMEOUT_BASE_PIXELS', 2_000_000_000)

	policy = predict_treecover._compute_tcd_timeout_policy()

	assert policy.timeout_seconds == 14400
	assert policy.input_pixels is None
	assert policy.capped is False


def test_tcd_timeout_policy_scales_large_ortho_to_cap(monkeypatch):
	monkeypatch.setattr(predict_treecover.settings, 'TCD_CONTAINER_TIMEOUT_SECONDS', 14400)
	monkeypatch.setattr(predict_treecover.settings, 'TCD_CONTAINER_TIMEOUT_MAX_SECONDS', 43200)
	monkeypatch.setattr(predict_treecover.settings, 'TCD_CONTAINER_TIMEOUT_BASE_PIXELS', 2_000_000_000)

	policy = predict_treecover._compute_tcd_timeout_policy(
		input_width=90377,
		input_height=104977,
	)

	assert policy.input_pixels == 9_487_506_329
	assert policy.timeout_seconds == 43200
	assert policy.capped is True


def test_tcd_container_wait_uses_adaptive_timeout_for_large_input(monkeypatch):
	client = _FakeDockerClient()

	monkeypatch.setattr(predict_treecover.docker, 'from_env', lambda: client)
	monkeypatch.setattr(
		predict_treecover,
		'build_container_forensics',
		lambda *args, **kwargs: {'dataset_id': kwargs['dataset_id'], 'stage': kwargs['stage']},
	)
	monkeypatch.setattr(predict_treecover, 'write_debug_bundle', lambda **_kwargs: None)
	monkeypatch.setattr(predict_treecover.settings, 'TCD_CONTAINER_TIMEOUT_SECONDS', 14400)
	monkeypatch.setattr(predict_treecover.settings, 'TCD_CONTAINER_TIMEOUT_MAX_SECONDS', 43200)
	monkeypatch.setattr(predict_treecover.settings, 'TCD_CONTAINER_TIMEOUT_BASE_PIXELS', 2_000_000_000)

	with pytest.raises(
		predict_treecover._TCDContainerTimeout,
		match='TCD container timed out after 12 hours',
	):
		predict_treecover._run_tcd_pipeline_container(
			'tcd_volume_10183_test',
			10183,
			'token',
			input_width=90377,
			input_height=104977,
		)

	assert len(client.containers.runs) == 1
	assert client.containers.runs[0].wait_timeout == 43200
