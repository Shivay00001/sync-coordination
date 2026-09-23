FROM python:3.10-slim
WORKDIR /app
# stdlib-only: no third-party dependencies to install
COPY sync_coordination.py ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "sync_coordination.py"]
