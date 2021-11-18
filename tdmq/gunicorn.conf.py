
# Gunicorn configuration

# timeout
# Default: 30
# Workers silent for more than this many seconds are killed and restarted.
#
# Keep this higher than the query timeout on the pgsql connection
timeout = 60
