import os
import sys
import requests
import psycopg
import time
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from functools import wraps
import logging

from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor


# Logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

load_dotenv()

app = Flask(__name__)


# --- OpenTelemetry Config ---
SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "flag-service")
OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "http://otel-collector.monitoring.svc.cluster.local:4318"
)

resource = Resource.create({
    ResourceAttributes.SERVICE_NAME: SERVICE_NAME,
    "service.version": os.getenv("SERVICE_VERSION", "1.0.0"),
    "deployment.environment": os.getenv("ENVIRONMENT", "dev"),
})

# Traces
trace_provider = TracerProvider(resource=resource)
trace_exporter = OTLPSpanExporter(
    endpoint=f"{OTEL_EXPORTER_OTLP_ENDPOINT}/v1/traces"
)
trace_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer(__name__)

# Metrics
metric_exporter = OTLPMetricExporter(
    endpoint=f"{OTEL_EXPORTER_OTLP_ENDPOINT}/v1/metrics"
)
metric_reader = PeriodicExportingMetricReader(
    exporter=metric_exporter,
    export_interval_millis=10000
)
metrics_provider = MeterProvider(
    resource=resource,
    metric_readers=[metric_reader]
)
metrics.set_meter_provider(metrics_provider)
meter = metrics.get_meter(__name__)

flag_operations_counter = meter.create_counter(
    name="flag_operations_total",
    description="Total de operações realizadas no serviço de flags",
    unit="1"
)

flag_errors_counter = meter.create_counter(
    name="flag_errors_total",
    description="Total de erros ocorridos no serviço de flags",
    unit="1"
)

flag_operation_duration = meter.create_histogram(
    name="flag_operation_duration_seconds",
    description="Tempo de execução das operações do serviço de flags",
    unit="s"
)

auth_validation_counter = meter.create_counter(
    name="flag_auth_validations_total",
    description="Total de validações de autenticação feitas pelo flag-service",
    unit="1"
)

auth_validation_duration = meter.create_histogram(
    name="flag_auth_validation_duration_seconds",
    description="Tempo gasto validando autenticação no auth-service",
    unit="s"
)

RequestsInstrumentor().instrument()


# --- Configuração ---
DATABASE_URL = os.getenv("DATABASE_URL")
AUTH_SERVICE_URL = os.getenv("AUTH_SERVICE_URL")

if not DATABASE_URL or not AUTH_SERVICE_URL:
    log.critical("Erro: DATABASE_URL e AUTH_SERVICE_URL devem ser definidos.")
    sys.exit(1)


# --- Pool de Conexão ---
try:
    pool = ConnectionPool(conninfo=DATABASE_URL, min_size=1, max_size=5)
    log.info("Pool de conexões com o PostgreSQL inicializado.")
except Exception as e:
    log.critical(f"Erro fatal ao conectar ao PostgreSQL: {e}")
    sys.exit(1)


# --- Middleware de Autenticação ---
def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        start_time = time.time()
        auth_header = request.headers.get("Authorization")

        with tracer.start_as_current_span("validate_auth_token") as span:
            span.set_attribute("auth.service.url", AUTH_SERVICE_URL)
            span.set_attribute("http.route", request.path)

            if not auth_header:
                auth_validation_counter.add(1, {
                    "status": "missing_header"
                })
                flag_errors_counter.add(1, {
                    "operation": "auth",
                    "error_type": "missing_authorization_header"
                })
                span.set_status(trace.Status(trace.StatusCode.ERROR, "Authorization header ausente"))
                return jsonify({"error": "Authorization header obrigatório"}), 401

            try:
                response = requests.get(
                    f"{AUTH_SERVICE_URL}/validate",
                    headers={"Authorization": auth_header},
                    timeout=3
                )

                duration = time.time() - start_time
                auth_validation_duration.record(duration, {
                    "status_code": str(response.status_code)
                })

                span.set_attribute("auth.status_code", response.status_code)

                if response.status_code != 200:
                    auth_validation_counter.add(1, {
                        "status": "invalid"
                    })
                    flag_errors_counter.add(1, {
                        "operation": "auth",
                        "error_type": "invalid_api_key"
                    })
                    span.set_status(trace.Status(trace.StatusCode.ERROR, "API key inválida"))
                    return jsonify({"error": "Chave de API inválida"}), 401

                auth_validation_counter.add(1, {
                    "status": "success"
                })
                span.set_status(trace.Status(trace.StatusCode.OK))

            except requests.exceptions.Timeout as e:
                duration = time.time() - start_time
                auth_validation_duration.record(duration, {
                    "status_code": "timeout"
                })
                auth_validation_counter.add(1, {
                    "status": "timeout"
                })
                flag_errors_counter.add(1, {
                    "operation": "auth",
                    "error_type": "auth_timeout"
                })
                span.record_exception(e)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                return jsonify({"error": "Serviço de autenticação indisponível (timeout)"}), 504

            except requests.exceptions.RequestException as e:
                duration = time.time() - start_time
                auth_validation_duration.record(duration, {
                    "status_code": "request_exception"
                })
                auth_validation_counter.add(1, {
                    "status": "unavailable"
                })
                flag_errors_counter.add(1, {
                    "operation": "auth",
                    "error_type": "auth_unavailable"
                })
                span.record_exception(e)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                return jsonify({"error": "Serviço de autenticação indisponível"}), 503

        return f(*args, **kwargs)

    return decorated


# --- Endpoints ---
@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/telemetry")
def telemetry_info():
    return jsonify({
        "service_name": SERVICE_NAME,
        "otel_endpoint": OTEL_EXPORTER_OTLP_ENDPOINT,
        "otlp_traces_path": f"{OTEL_EXPORTER_OTLP_ENDPOINT}/v1/traces",
        "otlp_metrics_path": f"{OTEL_EXPORTER_OTLP_ENDPOINT}/v1/metrics",
        "status": "otel-configured"
    })


@app.route("/flags", methods=["POST"])
@require_auth
def create_flag():
    start_time = time.time()

    with tracer.start_as_current_span("create_flag") as span:
        data = request.get_json()

        if not data or "name" not in data:
            flag_errors_counter.add(1, {
                "operation": "create",
                "error_type": "validation_error"
            })
            span.set_status(trace.Status(trace.StatusCode.ERROR, "name obrigatório"))
            return jsonify({"error": "'name' é obrigatório"}), 400

        name = data["name"]
        description = data.get("description", "")
        is_enabled = data.get("is_enabled", False)

        span.set_attribute("feature_flag.name", name)
        span.set_attribute("feature_flag.enabled", is_enabled)

        conn = None
        cur = None

        try:
            conn = pool.getconn()
            cur = conn.cursor(row_factory=dict_row)

            cur.execute(
                "INSERT INTO flags (name, description, is_enabled, created_at, updated_at) "
                "VALUES (%s, %s, %s, NOW(), NOW()) RETURNING *",
                (name, description, is_enabled)
            )

            new_flag = cur.fetchone()
            conn.commit()

            duration = time.time() - start_time
            flag_operations_counter.add(1, {
                "operation": "create",
                "status": "success"
            })
            flag_operation_duration.record(duration, {
                "operation": "create",
                "status": "success"
            })
            span.set_status(trace.Status(trace.StatusCode.OK))

            return jsonify(new_flag), 201

        except psycopg.errors.UniqueViolation as e:
            if conn:
                conn.rollback()

            duration = time.time() - start_time
            flag_errors_counter.add(1, {
                "operation": "create",
                "error_type": "unique_violation"
            })
            flag_operation_duration.record(duration, {
                "operation": "create",
                "status": "error"
            })
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))

            return jsonify({"error": f"Flag '{name}' já existe"}), 409

        except Exception as e:
            if conn:
                conn.rollback()

            duration = time.time() - start_time
            flag_errors_counter.add(1, {
                "operation": "create",
                "error_type": "internal_error"
            })
            flag_operation_duration.record(duration, {
                "operation": "create",
                "status": "error"
            })
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))

            return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500

        finally:
            if cur:
                cur.close()
            if conn:
                pool.putconn(conn)


@app.route("/flags", methods=["GET"])
@require_auth
def get_flags():
    start_time = time.time()

    with tracer.start_as_current_span("list_flags") as span:
        conn = None
        cur = None

        try:
            conn = pool.getconn()
            cur = conn.cursor(row_factory=dict_row)
            cur.execute("SELECT * FROM flags ORDER BY name")
            flags = cur.fetchall()

            duration = time.time() - start_time
            flag_operations_counter.add(1, {
                "operation": "list",
                "status": "success"
            })
            flag_operation_duration.record(duration, {
                "operation": "list",
                "status": "success"
            })
            span.set_attribute("feature_flag.count", len(flags))
            span.set_status(trace.Status(trace.StatusCode.OK))

            return jsonify(flags)

        except Exception as e:
            duration = time.time() - start_time
            flag_errors_counter.add(1, {
                "operation": "list",
                "error_type": "internal_error"
            })
            flag_operation_duration.record(duration, {
                "operation": "list",
                "status": "error"
            })
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))

            return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500

        finally:
            if cur:
                cur.close()
            if conn:
                pool.putconn(conn)


@app.route("/flags/<string:name>", methods=["GET"])
@require_auth
def get_flag(name):
    start_time = time.time()

    with tracer.start_as_current_span("get_flag") as span:
        span.set_attribute("feature_flag.name", name)

        conn = None
        cur = None

        try:
            conn = pool.getconn()
            cur = conn.cursor(row_factory=dict_row)
            cur.execute("SELECT * FROM flags WHERE name = %s", (name,))
            flag = cur.fetchone()

            duration = time.time() - start_time

            if not flag:
                flag_errors_counter.add(1, {
                    "operation": "get",
                    "error_type": "not_found"
                })
                flag_operation_duration.record(duration, {
                    "operation": "get",
                    "status": "not_found"
                })
                span.set_status(trace.Status(trace.StatusCode.ERROR, "Flag não encontrada"))
                return jsonify({"error": "Flag não encontrada"}), 404

            flag_operations_counter.add(1, {
                "operation": "get",
                "status": "success"
            })
            flag_operation_duration.record(duration, {
                "operation": "get",
                "status": "success"
            })
            span.set_status(trace.Status(trace.StatusCode.OK))

            return jsonify(flag)

        except Exception as e:
            duration = time.time() - start_time
            flag_errors_counter.add(1, {
                "operation": "get",
                "error_type": "internal_error"
            })
            flag_operation_duration.record(duration, {
                "operation": "get",
                "status": "error"
            })
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))

            return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500

        finally:
            if cur:
                cur.close()
            if conn:
                pool.putconn(conn)


@app.route("/flags/<string:name>", methods=["PUT"])
@require_auth
def update_flag(name):
    start_time = time.time()

    with tracer.start_as_current_span("update_flag") as span:
        span.set_attribute("feature_flag.name", name)

        data = request.get_json()

        if not data:
            flag_errors_counter.add(1, {
                "operation": "update",
                "error_type": "empty_body"
            })
            span.set_status(trace.Status(trace.StatusCode.ERROR, "corpo obrigatório"))
            return jsonify({"error": "Corpo da requisição obrigatório"}), 400

        fields = []
        values = []

        if "description" in data:
            fields.append("description = %s")
            values.append(data["description"])

        if "is_enabled" in data:
            fields.append("is_enabled = %s")
            values.append(data["is_enabled"])
            span.set_attribute("feature_flag.enabled", data["is_enabled"])

        if not fields:
            flag_errors_counter.add(1, {
                "operation": "update",
                "error_type": "validation_error"
            })
            span.set_status(trace.Status(trace.StatusCode.ERROR, "nenhum campo válido"))
            return jsonify({"error": "Pelo menos um campo ('description', 'is_enabled') é obrigatório"}), 400

        values.append(name)
        query = f"UPDATE flags SET {', '.join(fields)}, updated_at = NOW() WHERE name = %s RETURNING *"

        conn = None
        cur = None

        try:
            conn = pool.getconn()
            cur = conn.cursor(row_factory=dict_row)
            cur.execute(query, tuple(values))

            duration = time.time() - start_time

            if cur.rowcount == 0:
                flag_errors_counter.add(1, {
                    "operation": "update",
                    "error_type": "not_found"
                })
                flag_operation_duration.record(duration, {
                    "operation": "update",
                    "status": "not_found"
                })
                span.set_status(trace.Status(trace.StatusCode.ERROR, "Flag não encontrada"))
                return jsonify({"error": "Flag não encontrada"}), 404

            updated_flag = cur.fetchone()
            conn.commit()

            flag_operations_counter.add(1, {
                "operation": "update",
                "status": "success"
            })
            flag_operation_duration.record(duration, {
                "operation": "update",
                "status": "success"
            })
            span.set_status(trace.Status(trace.StatusCode.OK))

            return jsonify(updated_flag)

        except Exception as e:
            if conn:
                conn.rollback()

            duration = time.time() - start_time
            flag_errors_counter.add(1, {
                "operation": "update",
                "error_type": "internal_error"
            })
            flag_operation_duration.record(duration, {
                "operation": "update",
                "status": "error"
            })
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))

            return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500

        finally:
            if cur:
                cur.close()
            if conn:
                pool.putconn(conn)


@app.route("/flags/<string:name>", methods=["DELETE"])
@require_auth
def delete_flag(name):
    start_time = time.time()

    with tracer.start_as_current_span("delete_flag") as span:
        span.set_attribute("feature_flag.name", name)

        conn = None
        cur = None

        try:
            conn = pool.getconn()
            cur = conn.cursor()
            cur.execute("DELETE FROM flags WHERE name = %s", (name,))

            duration = time.time() - start_time

            if cur.rowcount == 0:
                flag_errors_counter.add(1, {
                    "operation": "delete",
                    "error_type": "not_found"
                })
                flag_operation_duration.record(duration, {
                    "operation": "delete",
                    "status": "not_found"
                })
                span.set_status(trace.Status(trace.StatusCode.ERROR, "Flag não encontrada"))
                return jsonify({"error": "Flag não encontrada"}), 404

            conn.commit()

            flag_operations_counter.add(1, {
                "operation": "delete",
                "status": "success"
            })
            flag_operation_duration.record(duration, {
                "operation": "delete",
                "status": "success"
            })
            span.set_status(trace.Status(trace.StatusCode.OK))

            return "", 204

        except Exception as e:
            if conn:
                conn.rollback()

            duration = time.time() - start_time
            flag_errors_counter.add(1, {
                "operation": "delete",
                "error_type": "internal_error"
            })
            flag_operation_duration.record(duration, {
                "operation": "delete",
                "status": "error"
            })
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))

            return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500

        finally:
            if cur:
                cur.close()
            if conn:
                pool.putconn(conn)


FlaskInstrumentor().instrument_app(app)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8002))
    app.run(host="0.0.0.0", port=port, debug=False)