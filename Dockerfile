FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# requirements.txt pins every runtime dependency and ends with `-e .`, so the
# package source has to be present before the install step.
COPY pyproject.toml README.md requirements.txt ./
COPY src ./src
RUN pip install -r requirements.txt

RUN useradd --system --uid 10001 logsman
USER logsman

EXPOSE 8010
CMD ["uvicorn", "logsman.main:app", "--host", "0.0.0.0", "--port", "8010", "--proxy-headers"]
