import pytest


from shared.supabase import login
from shared.settings import settings


@pytest.fixture(scope='session')
def auth_token():
	"""Provide authentication token for tests"""
	return login(settings.processor_username, settings.processor_password)
