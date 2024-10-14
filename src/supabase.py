from typing import Union, Generator, Literal, Optional
from contextlib import contextmanager

from pydantic import BaseModel
from supabase import create_client
from supabase.client import Client, ClientOptions
from gotrue import User

from .settings import settings


def login(user: str, password: str):
	"""Creates a supabase client instance and authorizes the user with login and password to
	return a supabase session.

	Args:
	    user (str): Supabase username as email
	    password (str): User password for supabase

	Returns:
	    AuthResponse: Returns a new supabase session if the login was successful
	"""
	# create a supabase client
	client = create_client(
		settings.supabase_url,
		settings.supabase_key,
		options=ClientOptions(auto_refresh_token=False),
	)

	client.auth.sign_in_with_password({'email': user, 'password': password})
	auth_response = client.auth.refresh_session()

	# client.auth.sign_out()
	# client.auth.close()
	# return the response
	return auth_response


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
