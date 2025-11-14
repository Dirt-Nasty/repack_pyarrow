import concurrent.futures
import os
import sys
import tempfile
import threading
import time
from typing import Iterable, List, Optional, Tuple

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.fs import S3FileSystem


def _ensure_no_leading_slash(path: str) -> str:
	if path.startswith("/"):
		return path[1:]
	return path


def _normalize_prefix(prefix: str) -> str:
	# Ensure no leading slash and ensure trailing slash for prefix math
	p = _ensure_no_leading_slash(prefix)
	if p != "" and not p.endswith("/"):
		p = p + "/"
	return p


def _rel_key(full_key: str, prefix: str) -> str:
	if not prefix:
		return full_key
	if not full_key.startswith(prefix):
		return full_key
	return full_key[len(prefix) :]


def list_parquet_keys(
	bucket: str,
	prefix: str,
	file_extensions: Tuple[str, ...] = (".parquet", ".parq"),
) -> List[str]:
	s3 = boto3.client("s3")
	prefix = _normalize_prefix(prefix)
	paginator = s3.get_paginator("list_objects_v2")
	keys: List[str] = []
	for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
		for obj in page.get("Contents", []):
			key = obj["Key"]
			lkey = key.lower()
			if any(lkey.endswith(ext) for ext in file_extensions):
				keys.append(key)
	return keys


def rewrite_repack_streaming(
	src_bucket: str,
	src_key: str,
	dst_bucket: str,
	dst_key: str,
	batch_size: int = 65536,
) -> None:
	"""
	Repack a single Parquet file by downloading it locally, rewriting, and uploading.

	This avoids relying on pyarrow's S3FileSystem for I/O, which can sometimes hang in
	certain environments, and instead uses boto3 for S3 network operations.
	"""
	s3 = boto3.client("s3")

	# Use local temp files; node has ample ephemeral storage on r6i.8xlarge.
	src_tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
	dst_tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
	src_tmp_path = src_tmp.name
	dst_tmp_path = dst_tmp.name
	src_tmp.close()
	dst_tmp.close()

	try:
		# Download source object
		s3.download_file(src_bucket, src_key, src_tmp_path)

		# Repack locally
		pf = pq.ParquetFile(src_tmp_path)
		schema = pf.schema_arrow
		writer = pq.ParquetWriter(dst_tmp_path, schema)
		try:
			for batch in pf.iter_batches(batch_size=batch_size):
				writer.write_batch(batch)
		finally:
			writer.close()

		# Upload repacked object
		s3.upload_file(dst_tmp_path, dst_bucket, dst_key)
	finally:
		# Best-effort cleanup of temp files
		for path in (src_tmp_path, dst_tmp_path):
			try:
				if path and os.path.exists(path):
					os.remove(path)
			except Exception:
				pass


def _should_skip_existing(
	bucket: str,
	key: str,
	skip_existing: bool,
) -> bool:
	if not skip_existing:
		return False
	s3 = boto3.client("s3")
	try:
		s3.head_object(Bucket=bucket, Key=key)
		return True
	except Exception:
		return False


def _copy_metadata_if_available(
	src_bucket: str,
	src_key: str,
	dst_bucket: str,
	dst_key: str,
) -> None:
	# Best-effort copy of content-type if present
	s3 = boto3.client("s3")
	try:
		head = s3.head_object(Bucket=src_bucket, Key=src_key)
		content_type = head.get("ContentType") or "application/octet-stream"
		# Set content-type metadata via copy with metadata-directive "REPLACE"
		s3.copy_object(
			Bucket=dst_bucket,
			Key=dst_key,
			CopySource={"Bucket": dst_bucket, "Key": dst_key},
			MetadataDirective="REPLACE",
			ContentType=content_type,
		)
	except Exception:
		# Silent best-effort
		return


def repack_prefix(
	src_bucket: str,
	src_prefix: str,
	dst_bucket: str,
	dst_prefix: str,
	max_workers: Optional[int] = None,
	batch_size: int = 65536,
	skip_existing: bool = True,
) -> Tuple[int, int]:
	src_prefix = _normalize_prefix(src_prefix)
	dst_prefix = _normalize_prefix(dst_prefix)
	keys = list_parquet_keys(src_bucket, src_prefix)
	total = len(keys)
	if total == 0:
		print(
			f"[INFO] No parquet files found under s3://{src_bucket}/{src_prefix} matching the filter; nothing to do."
		)
		return 0, 0

	s3fs = S3FileSystem(region=None)
	ok_counter = 0
	err_counter = 0
	ok_lock = threading.Lock()
	progress_step = max(1, total // 10)  # log progress roughly every 10%
	start_time = time.time()

	def _worker(src_key: str) -> None:
		nonlocal ok_counter, err_counter
		rel = _rel_key(src_key, src_prefix)
		dst_key = f"{dst_prefix}{rel}"
		print(
			f"[INFO] File start: s3://{src_bucket}/{src_key} -> s3://{dst_bucket}/{dst_key}"
		)
		if _should_skip_existing(dst_bucket, dst_key, skip_existing):
			with ok_lock:
				ok_counter += 1
				processed = ok_counter + err_counter
				if processed % progress_step == 0 or processed == total:
					elapsed = time.time() - start_time
					rate = processed / elapsed if elapsed > 0 else 0.0
					print(
						f"[INFO] Progress: {processed}/{total} files processed "
						f"in {elapsed:.1f}s ({rate:.1f} files/s)..."
					)
			return
		try:
			rewrite_repack_streaming(
				s3fs=s3fs,
				src_bucket=src_bucket,
				src_key=src_key,
				dst_bucket=dst_bucket,
				dst_key=dst_key,
				batch_size=batch_size,
			)
			_copy_metadata_if_available(src_bucket, src_key, dst_bucket, dst_key)
			print(
				f"[INFO] File done: s3://{src_bucket}/{src_key} -> s3://{dst_bucket}/{dst_key}"
			)
			with ok_lock:
				ok_counter += 1
				processed = ok_counter + err_counter
				if processed % progress_step == 0 or processed == total:
					elapsed = time.time() - start_time
					rate = processed / elapsed if elapsed > 0 else 0.0
					print(
						f"[INFO] Progress: {processed}/{total} files processed "
						f"in {elapsed:.1f}s ({rate:.1f} files/s)..."
					)
		except Exception as e:
			with ok_lock:
				err_counter += 1
				processed = ok_counter + err_counter
				if processed % progress_step == 0 or processed == total:
					elapsed = time.time() - start_time
					rate = processed / elapsed if elapsed > 0 else 0.0
					print(
						f"[INFO] Progress: {processed}/{total} files processed "
						f"in {elapsed:.1f}s ({rate:.1f} files/s)..."
					)
			print(f"[ERROR] {src_key} -> {dst_key}: {e}", file=sys.stderr)

	if max_workers is None or max_workers <= 0:
		# S3 throughput benefits from more threads; cap at a reasonable level
		from os import cpu_count as _cpu_count

		cpus = _cpu_count() or 4
		max_workers = min(32, 4 * cpus)

	print(
		f"[INFO] Starting repack of {total} files from s3://{src_bucket}/{src_prefix} "
		f"to s3://{dst_bucket}/{dst_prefix} with {max_workers} workers and batch_size={batch_size}."
	)

	with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
		list(executor.map(_worker, keys))

	total_elapsed = time.time() - start_time
	if total_elapsed <= 0:
		total_elapsed = 0.0
	success_rate = ok_counter / total_elapsed if total_elapsed > 0 else 0.0
	print(
		f"[INFO] Repack finished in {total_elapsed:.1f}s: "
		f"{ok_counter} succeeded, {err_counter} failed "
		f"({success_rate:.1f} files/s)."
	)

	return ok_counter, err_counter


