# syntax=docker/dockerfile:1
# Trial sandbox image. Locked. Base pinned by digest; packages pinned by version.
FROM python:3.12-slim@sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159
# Optional CA for networks behind a TLS proxy: build with MORPHOGEN_BUILD_CA set.
RUN --mount=type=secret,id=ca,required=false \
    if [ -s /run/secrets/ca ]; then export PIP_CERT=/run/secrets/ca; fi \
 && apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/* \
 && pip install --no-cache-dir pyyaml==6.0.3 \
 && useradd --create-home --uid 1000 trial
USER trial
WORKDIR /work
