FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY beautypipe ./beautypipe
RUN pip install --no-cache-dir .

USER 10001
ENTRYPOINT ["python", "-u", "-m"]
CMD ["beautypipe.consumer", "--help"]
