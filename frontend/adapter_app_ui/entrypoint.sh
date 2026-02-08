#!/bin/sh
set -e
# Install deps inside container (Linux) so native bins match. Volume preserves them.
npm install
exec npm run dev -- --host 0.0.0.0
