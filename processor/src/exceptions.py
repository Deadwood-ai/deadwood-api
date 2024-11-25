class ProcessorError(Exception):
	"""Base exception class for all processor errors"""

	def __init__(self, message: str, task_id: int | None = None, dataset_id: int | None = None):
		self.message = message
		self.task_id = task_id
		self.dataset_id = dataset_id
		super().__init__(self.message)


class AuthenticationError(ProcessorError):
	"""Raised when authentication/token validation fails"""

	def __init__(self, message: str = 'Authentication failed', token: str | None = None, **kwargs):
		self.token = token
		super().__init__(f'{message} (token: {token})', **kwargs)


class DatasetError(ProcessorError):
	"""Raised when there are issues with dataset operations"""

	def __init__(self, message: str, dataset_id: int, **kwargs):
		super().__init__(f'Dataset {dataset_id}: {message}', dataset_id=dataset_id, **kwargs)


class ProcessingError(ProcessorError):
	"""Raised when the actual processing of tasks fails"""

	def __init__(self, message: str, task_type: str, **kwargs):
		self.task_type = task_type
		super().__init__(f'{task_type} processing failed: {message}', **kwargs)


class StorageError(ProcessorError):
	"""Raised when storage operations (upload/download) fail"""

	def __init__(self, message: str, operation: str, file_path: str, **kwargs):
		self.operation = operation
		self.file_path = file_path
		super().__init__(f'Storage {operation} failed for {file_path}: {message}', **kwargs)
