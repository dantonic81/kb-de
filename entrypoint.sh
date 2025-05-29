#!/bin/sh

set -e

echo "✅ Postgres is assumed healthy from Compose healthcheck."

# Wait for 'migrate' container to finish
echo "⏳ Waiting for 'migrate' container to exit..."
while [ "$(docker inspect -f '{{.State.Running}}' migrate 2>/dev/null || echo false)" = "true" ]; do
  sleep 1
done
echo "✅ 'migrate' container has exited."

echo "🚀 Starting FastAPI..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
