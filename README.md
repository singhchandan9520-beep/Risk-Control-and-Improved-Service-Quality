"""
app_complete.py

Single-file implementation for:
  "Risk Management and Improve Service Quality"

Features:
- Flask API (risk CRUD, SLI endpoints, health, metrics)
- SQLite via SQLAlchemy (Risk, SLIRecord)
- Simple in-memory request recorder for demo metrics
- SLI calculator (availability, p95 latency, error rate)
- Background scheduler (APScheduler) to compute/persist SLI periodically
- Prometheus metrics endpoint (prometheus_client)
- CLI: --init-db, --run, --test

Usage:
  pip install -r requirements.txt
  python app_complete.py --init-db        # initialize local sqlite DB (risks.db)
  python app_complete.py --run            # run the Flask app
  python app_complete.py --test           # run built-in smoke tests
"""

import time
import threading
import argparse
import statistics
from datetime import datetime
from typing import List, Tuple

from flask import Flask, jsonify, request, abort
from flask_sqlalchemy import SQLAlchemy
from apscheduler.schedulers.background import BackgroundScheduler

# Prometheus client
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

# --------------------
# Configuration
# --------------------
DB_URI = "sqlite:///risks.db"
SLI_COMPUTE_INTERVAL_MINUTES = 5  # run periodic SLI compute every N minutes
REQUEST_LOG_KEEP = 20000  # keep at most this many request samples in memory

# --------------------
# Flask + DB Setup
# --------------------
app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = DB_URI
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


# --------------------
# Models
# --------------------
class Risk(db.Model):
    __tablename__ = "risks"
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(256), nullable=False)
    description = db.Column(db.Text, nullable=True)
    likelihood = db.Column(db.String(50), nullable=True)  # Low/Medium/High
    impact = db.Column(db.String(50), nullable=True)      # Low/Medium/High
    owner = db.Column(db.String(120), nullable=True)
    mitigation = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(50), default="Open")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "likelihood": self.likelihood,
            "impact": self.impact,
            "owner": self.owner,
            "mitigation": self.mitigation,
            "status": self.status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class SLIRecord(db.Model):
    __tablename__ = "sli_records"
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    availability = db.Column(db.Float, nullable=False)  # fraction e.g., 0.999
    latency_p95_ms = db.Column(db.Float, nullable=True)
    error_rate = db.Column(db.Float, nullable=True)

    def to_dict(self):
        return {
            "id": self.id,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "availability": self.availability,
            "latency_p95_ms": self.latency_p95_ms,
            "error_rate": self.error_rate,
        }


# --------------------
# In-memory Request Recorder (demo)
# --------------------
REQUEST_LOG: List[Tuple[float, int, int]] = []  # list of (timestamp_sec, latency_ms, status_code)
LOG_LOCK = threading.Lock()


def record_request(latency_ms: int, status_code: int):
    """Record a single request sample (for demo/testing)."""
    with LOG_LOCK:
        REQUEST_LOG.append((time.time(), int(latency_ms), int(status_code)))
        if len(REQUEST_LOG) > REQUEST_LOG_KEEP:
            # keep last half to avoid repeated shrink cost
            del REQUEST_LOG[: len(REQUEST_LOG) - int(REQUEST_LOG_KEEP / 2)]


def get_recent_requests(limit: int = 1000):
    """Return up to `limit` most recent request samples."""
    with LOG_LOCK:
        return list(REQUEST_LOG[-limit:])


# --------------------
# SLI Calculator Utilities
# --------------------
def compute_availability(requests: List[Tuple[float, int, int]], error_status_threshold: int = 500) -> float:
    """Return fraction of requests considered successful (status < error_status_threshold)."""
    if not requests:
        return 1.0
    good = sum(1 for r in requests if r[2] < error_status_threshold)
    return good / len(requests)


def compute_latency_p95(requests: List[Tuple[float, int, int]]) -> float:
    """Compute 95th percentile latency in ms. Return 0.0 if no samples."""
    latencies = [r[1] for r in requests if r[1] is not None]
    if not latencies:
        return 0.0
    # compute p95 robustly
    lat_sorted = sorted(latencies)
    # index for 95th percentile: using nearest-rank method
    k = max(0, min(len(lat_sorted) - 1, int(round(0.95 * (len(lat_sorted) - 1)))))
    return float(lat_sorted[k])


def compute_error_rate(requests: List[Tuple[float, int, int]], error_status_threshold: int = 500) -> float:
    """Return fraction of requests that are errors."""
    if not requests:
        return 0.0
    errors = sum(1 for r in requests if r[2] >= error_status_threshold)
    return errors / len(requests)


# --------------------
# Prometheus metrics (simple)
# --------------------
REQUEST_COUNTER = Counter("app_requests_total", "Total requests (demo)")
REQUEST_LATENCY = Histogram("app_request_latency_ms", "Request latency in ms (demo)")


# --------------------
# Background SLI compute & store
# --------------------
def compute_and_store_sli(limit: int = 2000):
    """Compute SLI values from recent request samples and persist a new SLIRecord."""
    reqs = get_recent_requests(limit=limit)
    avail = compute_availability(reqs)
    p95 = compute_latency_p95(reqs)
    err = compute_error_rate(reqs)
    rec = SLIRecord(availability=avail, latency_p95_ms=p95, error_rate=err)
    try:
        # need app context to write to DB
        with app.app_context():
            db.session.add(rec)
            db.session.commit()
    except Exception as e:
        # log to stdout (in production use proper logging)
        print(f"[SLI] failed to persist SLI: {e}")
    # print small log for demo
    print(f"[SLI] {datetime.utcnow().isoformat()} avail={avail:.4f} p95={p95:.1f} err={err:.4f}")


scheduler = None


def start_scheduler():
    global scheduler
    # scheduler will be no-op if already started
    if scheduler is not None:
        return
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: compute_and_store_sli(limit=2000), "interval", minutes=SLI_COMPUTE_INTERVAL_MINUTES, next_run_time=datetime.utcnow())
    scheduler.start()
    print(f"[Scheduler] started SLI compute every {SLI_COMPUTE_INTERVAL_MINUTES} minutes")


# --------------------
# Flask Routes
# --------------------
@app.before_first_request
def ensure_startup():
    # ensure DB exists and scheduler running
    with app.app_context():
        db.create_all()
    start_scheduler()


@app.route("/")
def index():
    start = time.time()
    # simulate small work (replace with real service code)
    time.sleep(0.004)  # 4 ms simulated work
    latency_ms = int((time.time() - start) * 1000)
    record_request(latency_ms, 200)
    REQUEST_COUNTER.inc()
    REQUEST_LATENCY.observe(latency_ms)
    return jsonify({"message": "ok", "latency_ms": latency_ms})


@app.route("/health")
def health():
    return jsonify({"status": "healthy", "time": datetime.utcnow().isoformat()})


# ---- Risk Register CRUD ----
@app.route("/risks", methods=["GET"])
def list_risks():
    risks = Risk.query.order_by(Risk.created_at.desc()).all()
    return jsonify([r.to_dict() for r in risks])


@app.route("/risks", methods=["POST"])
def create_risk():
    data = request.get_json() or {}
    if not data.get("title"):
        abort(400, "title required")
    r = Risk(
        title=data["title"],
        description=data.get("description"),
        likelihood=data.get("likelihood"),
        impact=data.get("impact"),
        owner=data.get("owner"),
        mitigation=data.get("mitigation"),
        status=data.get("status", "Open"),
    )
    db.session.add(r)
    db.session.commit()
    return jsonify({"id": r.id}), 201


@app.route("/risks/<int:risk_id>", methods=["PUT"])
def update_risk(risk_id: int):
    r = Risk.query.get_or_404(risk_id)
    data = request.get_json() or {}
    for field in ["title", "description", "likelihood", "impact", "owner", "mitigation", "status"]:
        if field in data:
            setattr(r, field, data[field])
    db.session.commit()
    return jsonify({"id": r.id})


@app.route("/risks/<int:risk_id>", methods=["DELETE"])
def delete_risk(risk_id: int):
    r = Risk.query.get_or_404(risk_id)
    db.session.delete(r)
    db.session.commit()
    return jsonify({"deleted": risk_id})


# ---- SLI endpoints ----
@app.route("/sli", methods=["GET"])
def current_sli():
    reqs = get_recent_requests(limit=1000)
    avail = compute_availability(reqs)
    p95 = compute_latency_p95(reqs)
    err = compute_error_rate(reqs)
    return jsonify({"availability": avail, "p95_latency_ms": p95, "error_rate": err})


@app.route("/sli/history", methods=["GET"])
def sli_history():
    recs = SLIRecord.query.order_by(SLIRecord.timestamp.desc()).limit(100).all()
    return jsonify([r.to_dict() for r in recs])


# Prometheus /metrics endpoint
@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}


# --------------------
# DB init helper & CLI
# --------------------
def init_db(db_uri: str = DB_URI):
    """Initialize sqlite DB (create tables)."""
    print(f"[DB] initializing database at {db_uri}")
    app.config["SQLALCHEMY_DATABASE_URI"] = db_uri
    with app.app_context():
        db.create_all()
    print("[DB] initialized")


# --------------------
# Simple built-in tests (smoke)
# --------------------
def run_tests():
    """Very small smoke tests to validate endpoints (no pytest required)."""
    print("[Test] Starting smoke tests...")
    # use Flask test client
    app.config["TESTING"] = True
    # use temp DB in memory for tests
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
    with app.app_context():
        db.create_all()

    client = app.test_client()

    # health
    r = client.get("/health")
    assert r.status_code == 200, "health endpoint failed"
    j = r.get_json()
    assert j["status"] == "healthy", "health status not healthy"

    # create risk
    r = client.post("/risks", json={"title": "Test risk", "impact": "High"})
    assert r.status_code == 201, f"create risk failed: {r.status_code}"
    data = r.get_json()
    risk_id = data["id"]

    # list risks
    r = client.get("/risks")
    assert r.status_code == 200
    j = r.get_json()
    assert any(x["id"] == risk_id for x in j), "created risk not found in list"

    # update
    r = client.put(f"/risks/{risk_id}", json={"status": "Mitigated"})
    assert r.status_code == 200
    j = r.get_json()
    assert j["id"] == risk_id

    # delete
    r = client.delete(f"/risks/{risk_id}")
    assert r.status_code == 200
    j = r.get_json()
    assert j["deleted"] == risk_id

    # metrics & sli endpoints (call index to produce some samples)
    client.get("/")  # produce sample
    client.get("/")
    r = client.get("/sli")
    assert r.status_code == 200
    sj = r.get_json()
    assert "availability" in sj

    r = client.get("/metrics")
    assert r.status_code == 200

    print("[Test] All smoke tests passed.")


# --------------------
# Run server
# --------------------
def run_server(host="0.0.0.0", port=8080, debug=False):
    # ensure DB exists and scheduler started prior to serving
    with app.app_context():
        db.create_all()
    start_scheduler()
    print(f"[Run] Starting Flask app on {host}:{port} (debug={debug})")
    app.run(host=host, port=port, debug=debug)


# --------------------
# CLI entrypoint
# --------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="app_complete.py - single-file risk & SLI demo")
    parser.add_argument("--init-db", action="store_true", help="Initialize the sqlite DB (risks.db)")
    parser.add_argument("--run", action="store_true", help="Run the Flask app")
    parser.add_argument("--test", action="store_true", help="Run built-in smoke tests")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind (default 0.0.0.0)")
    parser.add_argument("--port", default=8080, type=int, help="Port to bind (default 8080)")
    parser.add_argument("--debug", action="store_true", help="Run Flask in debug mode")
    args = parser.parse_args()

    if args.init_db:
        init_db(DB_URI)
    elif args.test:
        run_tests()
    elif args.run:
        run_server(host=args.host, port=args.port, debug=args.debug)
    else:
        parser.print_help()
