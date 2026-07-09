# Shared image for app / seeder / inspector. quixstreams itself is NOT baked in;
# it is bind-mounted read-only at /quix-streams and put on PYTHONPATH by
# entrypoint.sh, so every `docker run` exercises ArchDev's current working tree
# with NO image rebuild. Only quixstreams' heavy third-party deps are baked here
# (resolved from the library requirements.txt, kept in sync manually), so
# container start is a couple of seconds.
#
# See spec.md 5.3. NOTE (deviation): the spec proposes `pip install -e
# /quix-streams --no-deps` at start, but the mount is READ-ONLY (spec 4/5.1) and
# an editable install writes <pkg>.egg-info into the source tree, which fails on
# a ro mount. PYTHONPATH gives the identical live-tree/no-rebuild behaviour with
# zero writes to the mount.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=UTF-8
WORKDIR /app

# quixstreams runtime deps (from C:\repos\quix-streams-Main\requirements.txt) +
# python-dotenv (used by the app). Pinned to the library's own ranges.
RUN pip install --no-cache-dir \
    "confluent-kafka[avro,json,protobuf,schemaregistry]>=2.8.2,<2.12" \
    "rocksdict>=0.3,<0.4" \
    "typing_extensions>=4.8" \
    "orjson>=3.9,<4" \
    "pydantic>=2.7,<2.13" \
    "pydantic-settings>=2.3,<2.12" \
    "jsonschema>=4.3.0" \
    "jsonlines>=4,<5" \
    "rich>=13,<15" \
    "jsonpath_ng>=1.7.0,<2" \
    "httpx>=0.28.1" \
    "python-dotenv"

# Harness code (small; the only thing that triggers a rebuild when changed).
COPY app/ /app/app/
COPY seeder/ /app/seeder/
COPY inspect/ /app/inspect/
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["python", "/app/app/main.py"]
