import argparse
import sys
from typing import Tuple

from .repack import repack_prefix


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
	# Expect s3://bucket/prefix
	if not uri.startswith("s3://"):
		raise ValueError("URI must start with s3://")
	parts = uri[5:].split("/", 1)
	if len(parts) == 1:
		bucket, prefix = parts[0], ""
	else:
		bucket, prefix = parts[0], parts[1]
	return bucket, prefix


def main() -> int:
	parser = argparse.ArgumentParser(
		description="Repack Parquet files in S3 using pyarrow for Snowflake compatibility."
	)
	parser.add_argument(
		"--src",
		required=True,
		help="Source S3 prefix, e.g. s3://my-bucket/path/to/prefix",
	)
	parser.add_argument(
		"--dst",
		required=True,
		help="Destination S3 prefix, e.g. s3://my-bucket-repacked/path/to/prefix",
	)
	parser.add_argument(
		"--batch-size",
		type=int,
		default=65536,
		help="Arrow record batch size used when rewriting (default: 65536)",
	)
	parser.add_argument(
		"--workers",
		type=int,
		default=0,
		help="Max parallel workers (threads). 0 selects a sensible default.",
	)
	parser.add_argument(
		"--no-skip-existing",
		action="store_true",
		help="Do not skip when destination object already exists.",
	)

	args = parser.parse_args()
	src_bucket, src_prefix = _parse_s3_uri(args.src)
	dst_bucket, dst_prefix = _parse_s3_uri(args.dst)

	ok, err = repack_prefix(
		src_bucket=src_bucket,
		src_prefix=src_prefix,
		dst_bucket=dst_bucket,
		dst_prefix=dst_prefix,
		max_workers=args.workers,
		batch_size=args.batch_size,
		skip_existing=not args.no_skip_existing,
	)
	print(f"Completed: {ok}, Errors: {err}")
	return 0 if err == 0 else 2


if __name__ == "__main__":
	sys.exit(main())


