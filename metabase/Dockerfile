FROM metabase/metabase:latest

# Health check to verify metabase is functioning
HEALTHCHECK --start-period=30s --interval=1m --timeout=10s \
  CMD curl -f http://localhost:3000/api/health || exit 1