FROM python:3.15.0-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY pyproject.toml ./
COPY relay ./relay
RUN pip install --no-cache-dir .
RUN useradd --uid 10001 --create-home relay
USER relay
CMD ["uvicorn", "relay.api:app", "--host", "0.0.0.0", "--port", "8080"]
