FROM python:3.11-slim

WORKDIR /src

ENV PYTHONUNBUFFERED=1

COPY requirements.txt /src/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY repack_s3 /src/repack_s3
COPY docker-entrypoint.sh /src/docker-entrypoint.sh
RUN chmod +x /src/docker-entrypoint.sh

ENTRYPOINT ["/src/docker-entrypoint.sh"]


