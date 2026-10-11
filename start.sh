#!/bin/sh
# Demo container entrypoint: Postgres, RabbitMQ, the watcher and workers (supervised), then the API.
set -e
PGV=$(ls /usr/lib/postgresql | sort -n | tail -1)
PGBIN=/usr/lib/postgresql/$PGV/bin
export PGDATA=/tmp/pgdata
mkdir -p $PGDATA && chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres --auth=trust -E UTF8 >/dev/null"
su postgres -c "$PGBIN/pg_ctl -D $PGDATA -o '-c listen_addresses=127.0.0.1 -c port=5432 -c unix_socket_directories=/tmp -c shared_buffers=16MB -c max_connections=60' -l /tmp/pg.log -w start"
psql -h 127.0.0.1 -U postgres -c "CREATE DATABASE opp" >/dev/null
export DATABASE_URL=postgresql://postgres@127.0.0.1:5432/opp

# RabbitMQ, trimmed for a 512 MB instance.
mkdir -p /etc/rabbitmq
cat > /etc/rabbitmq/rabbitmq.conf <<CONF
vm_memory_high_watermark.absolute = 160MB
disk_free_limit.absolute = 50MB
listeners.tcp.default = 127.0.0.1:5672
CONF
echo "[]." > /etc/rabbitmq/enabled_plugins
export RABBITMQ_NODENAME=rabbit@localhost RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="+S 1:1 +A 2 +P 65536"
rabbitmq-server > /tmp/rabbit.log 2>&1 &
for i in $(seq 1 60); do
  rabbitmq-diagnostics -q check_port_connectivity >/dev/null 2>&1 && break
  sleep 1
done
export AMQP_URL=amqp://guest:guest@127.0.0.1:5672/%2F
export OPP_DROP_DIR=/tmp/drop OPP_WORKERS=${OPP_WORKERS:-3} OPP_WORK_SECONDS=${OPP_WORK_SECONDS:-1.0}
python -c "from opp import db; db.init_schema()"
python -m opp.supervise > /tmp/workers.log 2>&1 &
exec uvicorn opp.api:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1
