import os

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://opp@127.0.0.1:5434/opp")
AMQP_URL = os.environ.get("AMQP_URL", "amqp://guest:guest@127.0.0.1:5672/%2F")
DROP_DIR = os.environ.get("OPP_DROP_DIR", os.path.abspath("drop"))
# Seconds a file's size and mtime must stay unchanged before it counts as ready.
STABLE_SECONDS = float(os.environ.get("OPP_STABLE_SECONDS", "1.0"))
# Simulated analysis time per run. Real Smart-Action-style runs take minutes; the demo uses seconds.
WORK_SECONDS = float(os.environ.get("OPP_WORK_SECONDS", "1.5"))
LEASE_SECONDS = float(os.environ.get("OPP_LEASE_SECONDS", "4"))
RUN_QUEUE = "opp.runs"
PARKED_QUEUE = "opp.parked"
DLX = "opp.dlx"
