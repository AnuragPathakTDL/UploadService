#!/bin/sh
set -euo pipefail

if [ -z "${DATABASE_URL:-}" ]; then
	echo "DATABASE_URL must be set" >&2
	exit 1
fi

PRISMA_DB_SYNC=${PRISMA_DB_SYNC:-true}

if [ "$PRISMA_DB_SYNC" = "true" ]; then
	echo "Syncing database schema via Prisma..."
	npx prisma db push --schema="prisma/schema.prisma" --skip-generate
fi

exec node dist/server.js
