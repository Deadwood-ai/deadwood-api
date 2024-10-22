from typing import Union, Literal, Optional, Generator
from contextlib import contextmanager
import time

from pydantic import BaseModel
from supabase import create_client
from supabase.client import Client, ClientOptions
from gotrue import User

from .settings import settings

# Global variable to store the cached session
cached_session = None


def login(user: str, password: str) -> str:
	"""
	Creates a supabase client instance, authorizes the user with login and password,
	and manages session caching and refreshing.

	Args:
	    user (str): Supabase username as email
	    password (str): User password for supabase

	Returns:
	    str: Returns a valid access token
	"""
	global cached_session

	client = create_client(
		settings.supabase_url,
		settings.supabase_key,
		options=ClientOptions(auto_refresh_token=False),
	)

	current_time = int(time.time())
	threshold = 300  # 5 minutes before expiration

	if cached_session:
		print('found cached session')
		if cached_session.session.expires_at > (current_time + threshold):
			print('session is still valid')
			return cached_session.session.access_token
		else:
			print('session is expired, refreshing')
			try:
				refreshed_session = client.auth.refresh_session()
				cached_session = refreshed_session
				print('session refreshed')
				return cached_session.session.access_token
			except Exception:
				print('session refresh failed, clearing cache')
				cached_session = None

	# If no valid cached session, perform a new login
	try:
		auth_response = client.auth.sign_in_with_password({'email': user, 'password': password})
		cached_session = auth_response
		# print('new session created and cached')
		return cached_session.session.access_token
	except Exception as e:
		raise Exception(f'Login failed: {str(e)}')


def verify_token(jwt: str) -> Union[Literal[False], User]:
	"""Verifies a user jwt token string against the active supabase sessions

	Args:
	    jwt (str): A jwt token string

	Returns:
	    Union[Literal[False], User]: Returns true if user session is active, false if not
	"""
	# make the authentication
	with use_client(jwt) as client:
		response = client.auth.get_user(jwt)

	# check the token
	try:
		return response.user
	except Exception:
		return False


@contextmanager
def use_client(access_token: Optional[str] = None) -> Generator[Client, None, None]:
	"""Creates and returns a supabase client session

	Args:
	    access_token (Optional[str], optional): Optional access token. Defaults to None.

	Yields:
	    Generator[Client, None, None]: A supabase client session
	"""
	# create a supabase client
	client = create_client(
		settings.supabase_url,
		settings.supabase_key,
		options=ClientOptions(auto_refresh_token=False),
	)

	# yield the client
	try:
		# set the access token to the postgrest (rest-api) client if available
		if access_token is not None:
			client.postgrest.auth(token=access_token)

		yield client
	finally:
		# client.auth.sign_out()
		# client.auth.close()
		pass


class SupabaseReader(BaseModel):
	Model: type[BaseModel]
	table: str
	token: str | None = None

	def by_id(self, dataset_id: int) -> BaseModel | None:
		"""Reads an instance from the bound model from
		supabase.
		"""
		# figure out the primary filed
		if 'id' in self.Model.model_fields:
			id_field = 'id'
		elif 'dataset_id' in self.Model.model_fields:
			id_field = 'dataset_id'
		else:
			raise AttributeError('Model does not have an id field')

		with use_client(self.token) as client:
			result = client.table(self.table).select('*').eq(id_field, dataset_id).execute()

		if len(result.data) == 0:
			return None

		return self.Model(**result.data[0])
