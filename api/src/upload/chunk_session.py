"""Durable chunk receipts on the API server's local storage volume.

One lock covers receipt updates and finalization across threads and API workers.
Never delete a session/lock on completion: late requests must see its receipt,
and replacing a lock inode would let two workers enter the same session.
"""

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
from typing import Literal

from fastapi import HTTPException
from pydantic import BaseModel, Field


class ChunkReceipt(BaseModel):
	size: int
	sha256: str


class UploadReceipt(BaseModel):
	user_id: str
	contract_hash: str
	chunks: list[ChunkReceipt] = Field(default_factory=list)
	phase: Literal['receiving', 'finalizing', 'complete'] = 'receiving'
	response: dict | None = None


def _sync_directory(path: Path):
	directory = os.open(path, os.O_RDONLY)
	try:
		os.fsync(directory)
	finally:
		os.close(directory)


class ChunkSession:
	"""Own a locked upload's assembly, durable receipts, and final response."""

	def __init__(self, receipt_path: Path, target_path: Path, user_id: str, contract: dict):
		self.receipt_path = receipt_path
		self.target_path = target_path
		self.chunks_total = contract['chunks_total']
		contract_hash = hashlib.sha256(json.dumps(contract, sort_keys=True).encode()).hexdigest()
		if receipt_path.exists():
			self.receipt = UploadReceipt.model_validate_json(receipt_path.read_bytes())
			if self.receipt.user_id != user_id:
				raise HTTPException(status_code=409, detail='Upload ID is already in use')
			if self.receipt.contract_hash != contract_hash:
				raise HTTPException(status_code=409, detail='Upload metadata or chunk count changed')
		else:
			if target_path.exists():
				raise HTTPException(
					status_code=409, detail='Upload has no retry receipts; restart with a new upload ID'
				)
			self.receipt = UploadReceipt(user_id=user_id, contract_hash=contract_hash)

	def _save(self):
		"""Replace the receipt atomically, after its referenced bytes are durable."""
		temporary = self.receipt_path.with_suffix('.pending')
		with temporary.open('w') as output:
			output.write(self.receipt.model_dump_json())
			output.flush()
			os.fsync(output.fileno())
		temporary.replace(self.receipt_path)
		_sync_directory(self.receipt_path.parent)

	def accept(self, index: int, content: bytes) -> dict | None:
		"""Return an existing reply, or None when all bytes need finalization."""
		chunk = ChunkReceipt(size=len(content), sha256=hashlib.sha256(content).hexdigest())
		if not content:
			raise HTTPException(status_code=422, detail='Empty upload chunk')
		if index < len(self.receipt.chunks):
			if self.receipt.chunks[index] != chunk:
				raise HTTPException(status_code=409, detail='Chunk content changed on retry')
		elif index != len(self.receipt.chunks):
			raise HTTPException(status_code=409, detail='Chunks must arrive in order')
		elif self.receipt.phase == 'receiving':
			# Persist the session before creating any data, including the first chunk.
			# A crashed write can then be truncated to the last committed receipt.
			if not self.receipt_path.exists():
				self._save()
			offset = sum(part.size for part in self.receipt.chunks)
			if offset and (not self.target_path.exists() or self.target_path.stat().st_size < offset):
				raise HTTPException(status_code=409, detail='Upload data is missing; restart with a new upload ID')
			mode = 'r+b' if self.target_path.exists() else 'w+b'
			with self.target_path.open(mode) as output:
				output.truncate(offset)
				output.seek(offset)
				output.write(content)
				output.flush()
				os.fsync(output.fileno())
			_sync_directory(self.target_path.parent)
			self.receipt.chunks.append(chunk)
			self._save()

		if self.receipt.phase == 'finalizing':
			raise HTTPException(
				status_code=409,
				detail='Upload finalization was interrupted; contact support before starting another upload',
			)
		if index == self.chunks_total - 1:
			return self.receipt.response
		return {'message': f'Chunk {index} of {self.chunks_total} received'}

	def begin_finalization(self):
		if len(self.receipt.chunks) != self.chunks_total or self.receipt.phase != 'receiving':
			raise HTTPException(status_code=409, detail='Upload is not ready for finalization')
		expected_size = sum(part.size for part in self.receipt.chunks)
		if not self.target_path.exists() or self.target_path.stat().st_size < expected_size:
			raise HTTPException(status_code=409, detail='Upload data is missing; restart with a new upload ID')
		# Discard bytes from an interrupted, uncommitted append before processing.
		with self.target_path.open('r+b') as output:
			output.truncate(expected_size)
			output.flush()
			os.fsync(output.fileno())
		# The DB and filesystem are not one transaction. Persist a fence before
		# any DB insert so an ambiguous failure never repeats those side effects.
		self.receipt.phase = 'finalizing'
		self._save()

	def complete(self, response: dict):
		# The processors rename within this directory; make that rename durable
		# before promising a replayable successful response.
		_sync_directory(self.target_path.parent)
		self.receipt.response = response
		self.receipt.phase = 'complete'
		self._save()


@contextmanager
def locked_chunk_session(root: Path, upload_id: str, target_path: Path, user_id: str, contract: dict):
	root.mkdir(mode=0o700, parents=True, exist_ok=True)
	_sync_directory(root.parent)
	with (root / f'{upload_id}.lock').open('a+b') as lock:
		try:
			fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
		except BlockingIOError:
			raise HTTPException(
				status_code=503, detail='Upload request already in progress', headers={'Retry-After': '2'}
			)
		try:
			yield ChunkSession(root / f'{upload_id}.json', target_path, user_id, contract)
		finally:
			fcntl.flock(lock, fcntl.LOCK_UN)
