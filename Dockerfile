# syntax=docker/dockerfile:1.7
# One image for every role; the subcommand picks ingest / writer / alerter.

FROM python:3.11-slim AS build
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /src
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip wheel --wheel-dir /wheels .

FROM python:3.11-slim AS runtime
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
RUN groupadd --system --gid 10001 app && useradd --system --uid 10001 --gid app --no-create-home app
COPY --from=build /wheels /wheels
RUN pip install --no-index --find-links=/wheels flightline && rm -rf /wheels
WORKDIR /app
COPY config ./config
USER 10001
EXPOSE 8080 9100
ENTRYPOINT ["flightline"]
CMD ["ingest"]
