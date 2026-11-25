#!/bin/bash
# Hive metastore: create the schema on an empty metastore DB (first start), then run the service.
# (A one-line "bash -c ..." command does not survive entrypoint.sh's unquoted `exec $@`.)
if ! /opt/hive/bin/schematool -dbType postgres -info >/dev/null 2>&1; then
  echo "metastore schema not found -> schematool -initSchema"
  /opt/hive/bin/schematool -dbType postgres -initSchema || exit 1
fi
exec /opt/hive/bin/hive --service metastore
