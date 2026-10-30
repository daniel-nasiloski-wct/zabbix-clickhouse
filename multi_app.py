"""Simula 4 aplicações de clientes diferentes, com 17 serviços, e envia traces, métricas e logs via OTLP/gRPC.

Cada aplicação tem um serviço de entrada que chama os outros, então cada requisição gera um trace distribuído:
  - loja-online:    checkout-demo -> auth-service, payment-gateway, shipping-service, catalog-api
  - banco-digital:  bank-portal -> account-service, fraud-engine, notification-service
  - streaming:      stream-hub -> entitlement-service, media-service, recommendation-service
  - logistica:      fleet-tracker -> telemetry-ingest, routing-engine -> traffic-service
Com --incident, um serviço sorteado fica lento ou começa a falhar durante toda a execução.

Uso:
    .venv/bin/python multi_app.py [--endpoint http://192.168.128.101:4317] [--iterations 30] [--interval 2] [--incident]
"""

import argparse
import logging
import random
import time

from opentelemetry import context
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode

# serviço -> cliente (service.namespace)
TENANTS = {
    "checkout-demo": "loja-online", "catalog-api": "loja-online", "auth-service": "loja-online",
    "payment-gateway": "loja-online", "shipping-service": "loja-online",
    "bank-portal": "banco-digital", "account-service": "banco-digital", "fraud-engine": "banco-digital",
    "notification-service": "banco-digital",
    "stream-hub": "streaming", "entitlement-service": "streaming", "media-service": "streaming",
    "recommendation-service": "streaming",
    "fleet-tracker": "logistica", "telemetry-ingest": "logistica", "routing-engine": "logistica",
    "traffic-service": "logistica",
}
SERVICES = list(TENANTS)
MODES = ["latency", "errors"]


class Service:
    def __init__(self, name, endpoint):
        self.name = name
        resource = Resource.create({"service.name": name, "service.namespace": TENANTS[name], "service.version": "1.0.0",
                                    "deployment.environment": "lab", "host.name": "multi-app"})
        self.tp = TracerProvider(resource=resource)
        self.tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
        self.tracer = self.tp.get_tracer(f"{name}.tracer", "1.0.0")
        self.mp = MeterProvider(resource=resource, metric_readers=[PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=endpoint, insecure=True), export_interval_millis=5000)])
        meter = self.mp.get_meter(f"{name}.meter", "1.0.0")
        self.requests = meter.create_counter("app.requests", unit="1", description="Total de requisições")
        self.duration = meter.create_histogram("app.request.duration", unit="ms",
                                               description="Duração das requisições")
        self.lp = LoggerProvider(resource=resource)
        self.lp.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter(endpoint=endpoint, insecure=True)))
        self.log = logging.getLogger(name)
        self.log.setLevel(logging.INFO)
        self.log.propagate = False
        self.log.addHandler(LoggingHandler(level=logging.INFO, logger_provider=self.lp))

    def shutdown(self):
        for p in (self.tp, self.mp, self.lp):
            p.force_flush()
            p.shutdown()


class Failure(Exception):
    pass


incident = {}  # {"service": ..., "mode": ...} quando --incident


def degraded(svc, mode):
    return incident.get("service") == svc and incident.get("mode") == mode


def step(svc, name, kind, low, high, fail_rate=0.0, slow=None, error_msg="falha", attrs=None):
    """Um span filho: dorme entre low e high segundos e falha com fail_rate."""
    with svc.tracer.start_as_current_span(name, kind=kind, attributes=attrs or {}) as span:
        if slow and degraded(svc.name, "latency"):
            low, high = slow
        time.sleep(random.uniform(low, high))
        if degraded(svc.name, "errors"):
            fail_rate = 0.8
        if random.random() < fail_rate:
            span.set_status(Status(StatusCode.ERROR, error_msg))
            raise Failure(f"{name}: {error_msg}")


def serve(svc, name, handler, attrs=None):
    """Span SERVER de uma requisição recebida pelo serviço, com log e métricas."""
    start = time.perf_counter()
    with svc.tracer.start_as_current_span(name, kind=SpanKind.SERVER, attributes=attrs or {}) as span:
        try:
            handler()
            status, failed = 200, None
        except Failure as e:
            status, failed = 500, e
        span.set_attribute("http.response.status_code", status)
        if failed:
            span.set_status(Status(StatusCode.ERROR, str(failed)))
            svc.log.error("%s falhou: %s", name, failed)
        else:
            svc.log.info("%s concluída", name)
        elapsed_ms = (time.perf_counter() - start) * 1000
        if elapsed_ms > 500:
            svc.log.warning("%s lenta: %.0f ms", name, elapsed_ms)
        a = {"http.route": name, "http.response.status_code": status}
        svc.requests.add(1, a)
        svc.duration.record(elapsed_ms, a)
    if failed:
        raise failed


def call(caller, name, callee_fn):
    """Span CLIENT no chamador envolvendo a chamada a outro serviço."""
    with caller.tracer.start_as_current_span(name, kind=SpanKind.CLIENT) as span:
        try:
            callee_fn()
        except Failure:
            span.set_status(Status(StatusCode.ERROR, f"{name} falhou"))
            raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://192.168.128.101:4317", help="proxy APM do Zabbix")
    parser.add_argument("--iterations", type=int, default=30)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--incident", action="store_true",
                        help="deixa um dos 17 serviços, sorteado, lento ou com erros durante a execução")
    args = parser.parse_args()

    s = {name: Service(name, args.endpoint) for name in SERVICES}
    checkout, catalog, auth, payment, shipping = (s[n] for n in SERVICES[:5])
    if args.incident:
        incident.update(service=random.choice(SERVICES), mode=random.choice(MODES))
        kind = "latência alta" if incident["mode"] == "latency" else "erros"
        print(f"Incidente: {incident['service']} com {kind}", flush=True)

    def auth_validate():
        serve(auth, "POST /validate", lambda: step(auth, "token.verify", SpanKind.INTERNAL, 0.005, 0.02,
                                                   0.01, slow=(0.4, 0.9), error_msg="token inválido"))

    def auth_login():
        def h():
            step(auth, "ldap.bind", SpanKind.CLIENT, 0.02, 0.06, 0.03, slow=(0.6, 1.2), error_msg="LDAP indisponível",
                 attrs={"server.address": "ldap.lab"})
            step(auth, "token.sign", SpanKind.INTERNAL, 0.002, 0.01)
        serve(auth, "POST /login", h)

    def catalog_list():
        def h():
            step(catalog, "cache.get", SpanKind.CLIENT, 0.001, 0.005, attrs={"db.system": "redis"})
            if random.random() < 0.3 or degraded("catalog-api", "latency"):
                step(catalog, "db.query", SpanKind.CLIENT, 0.03, 0.09, 0.03, slow=(0.5, 1.1),
                     error_msg="timeout no PostgreSQL", attrs={"db.system": "postgresql"})
        serve(catalog, "GET /catalog", h)

    def payment_charge():
        def h():
            step(payment, "fraud.check", SpanKind.INTERNAL, 0.01, 0.03)
            step(payment, "bank.api", SpanKind.CLIENT, 0.05, 0.15, 0.05, slow=(0.8, 1.5),
                 error_msg="banco recusou a transação", attrs={"server.address": "api.banco.lab"})
        serve(payment, "POST /charge", h)

    def shipping_create():
        def h():
            step(shipping, "inventory.reserve", SpanKind.CLIENT, 0.01, 0.04, 0.02, error_msg="estoque indisponível",
                 attrs={"db.system": "mysql"})
            step(shipping, "carrier.quote", SpanKind.CLIENT, 0.04, 0.12, 0.03, slow=(0.7, 1.4),
                 error_msg="transportadora não respondeu", attrs={"server.address": "api.transportadora.lab"})
        serve(shipping, "POST /shipment", h)

    def checkout_request():
        route = random.choice(["/checkout", "/cart", "/products"])

        def h():
            if route == "/checkout":
                call(checkout, "auth.validate", auth_validate)
                step(checkout, "db.query", SpanKind.CLIENT, 0.01, 0.05, 0.02, slow=(0.5, 1.0),
                     error_msg="timeout no MySQL", attrs={"db.system": "mysql"})
                call(checkout, "payment.authorize", payment_charge)
                call(checkout, "shipping.create", shipping_create)
            elif route == "/cart":
                step(checkout, "db.query", SpanKind.CLIENT, 0.01, 0.05, 0.02, slow=(0.5, 1.0),
                     error_msg="timeout no MySQL", attrs={"db.system": "mysql"})
            else:
                call(checkout, "catalog.list", catalog_list)
        serve(checkout, f"GET {route}", h, {"http.request.method": "GET", "url.path": route})


    # banco-digital
    bank, account, fraud, notify = (s[n] for n in ("bank-portal", "account-service", "fraud-engine",
                                                   "notification-service"))

    def account_balance():
        serve(account, "GET /balance", lambda: step(account, "db.query", SpanKind.CLIENT, 0.01, 0.04, 0.01,
                                                     slow=(0.5, 1.0), error_msg="timeout no PostgreSQL",
                                                     attrs={"db.system": "postgresql"}))

    def account_debit():
        def h():
            step(account, "ledger.write", SpanKind.CLIENT, 0.03, 0.08, 0.02, slow=(0.7, 1.3),
                 error_msg="lock no ledger", attrs={"db.system": "postgresql"})
            step(account, "audit.log", SpanKind.INTERNAL, 0.002, 0.008)
        serve(account, "POST /debit", h)

    def fraud_score():
        def h():
            step(fraud, "feature.lookup", SpanKind.CLIENT, 0.002, 0.01, attrs={"db.system": "redis"})
            step(fraud, "model.predict", SpanKind.INTERNAL, 0.02, 0.06, 0.02, slow=(0.6, 1.2),
                 error_msg="modelo indisponível")
        serve(fraud, "POST /score", h)

    def notify_send():
        def h():
            step(notify, "template.render", SpanKind.INTERNAL, 0.002, 0.008)
            step(notify, "sms.send", SpanKind.CLIENT, 0.04, 0.1, 0.03, slow=(0.8, 1.5),
                 error_msg="provedor de SMS falhou", attrs={"server.address": "api.sms.lab"})
        serve(notify, "POST /notify", h)

    def bank_request():
        route = random.choices(["POST /transfer", "GET /balance"], [0.6, 0.4])[0]

        def h():
            step(bank, "session.check", SpanKind.INTERNAL, 0.002, 0.008, 0.01, slow=(0.4, 0.8),
                 error_msg="sessão expirada")
            if route == "POST /transfer":
                call(bank, "fraud.score", fraud_score)
                call(bank, "account.debit", account_debit)
                call(bank, "notification.send", notify_send)
            else:
                call(bank, "account.balance", account_balance)
        serve(bank, route, h)

    # streaming
    stream, entitle, media, recommend = (s[n] for n in ("stream-hub", "entitlement-service", "media-service",
                                                        "recommendation-service"))

    def entitlement_check():
        def h():
            step(entitle, "cache.get", SpanKind.CLIENT, 0.001, 0.005, attrs={"db.system": "redis"})
            step(entitle, "plan.check", SpanKind.INTERNAL, 0.003, 0.01, 0.01, slow=(0.4, 0.9),
                 error_msg="plano não encontrado")
        serve(entitle, "GET /entitlement", h)

    def media_manifest():
        def h():
            step(media, "object.get", SpanKind.CLIENT, 0.02, 0.07, 0.02, slow=(0.8, 1.6),
                 error_msg="objeto não encontrado no storage", attrs={"server.address": "s3.lab"})
            step(media, "cdn.sign", SpanKind.CLIENT, 0.01, 0.03, 0.01, attrs={"server.address": "cdn.video.lab"})
        serve(media, "GET /manifest", h)

    def recommend_list():
        def h():
            step(recommend, "db.query", SpanKind.CLIENT, 0.02, 0.06, 0.02, slow=(0.6, 1.2),
                 error_msg="timeout no Cassandra", attrs={"db.system": "cassandra"})
            step(recommend, "model.rank", SpanKind.INTERNAL, 0.01, 0.04)
        serve(recommend, "GET /recommendations", h)

    def stream_request():
        route = random.choices(["GET /play", "GET /home"], [0.6, 0.4])[0]

        def h():
            call(stream, "entitlement.check", entitlement_check)
            if route == "GET /play":
                call(stream, "media.manifest", media_manifest)
            else:
                call(stream, "recommendation.list", recommend_list)
        serve(stream, route, h)

    # logistica
    fleet, ingest, routing, traffic = (s[n] for n in ("fleet-tracker", "telemetry-ingest", "routing-engine",
                                                      "traffic-service"))

    def ingest_positions():
        def h():
            step(ingest, "payload.validate", SpanKind.INTERNAL, 0.001, 0.004, 0.01, error_msg="payload inválido")
            step(ingest, "kafka.produce", SpanKind.CLIENT, 0.005, 0.02, 0.01, slow=(0.5, 1.1),
                 error_msg="broker indisponível", attrs={"server.address": "kafka.lab"})
        serve(ingest, "POST /positions", h)

    def traffic_status():
        def h():
            step(traffic, "cache.get", SpanKind.CLIENT, 0.001, 0.005, attrs={"db.system": "redis"})
            step(traffic, "feed.fetch", SpanKind.CLIENT, 0.03, 0.08, 0.03, slow=(0.7, 1.4),
                 error_msg="feed de trânsito fora do ar", attrs={"server.address": "api.transito.lab"})
        serve(traffic, "GET /traffic", h)

    def routing_route():
        def h():
            step(routing, "geo.query", SpanKind.CLIENT, 0.02, 0.05, 0.01, slow=(0.6, 1.2),
                 error_msg="timeout no PostGIS", attrs={"db.system": "postgresql"})
            call(routing, "traffic.status", traffic_status)
            step(routing, "maps.directions", SpanKind.CLIENT, 0.04, 0.1, 0.02, attrs={"server.address": "api.mapas.lab"})
        serve(routing, "GET /route", h)

    def fleet_request():
        route = random.choice(["POST /positions", "GET /route"])

        def h():
            if route == "POST /positions":
                call(fleet, "telemetry.ingest", ingest_positions)
            else:
                call(fleet, "routing.route", routing_route)
        serve(fleet, route, h)

    # Por iteração: uma requisição na entrada de cada aplicação. Na loja, também uma consulta direta ao
    # catálogo, um login e uma cobrança e um frete avulsos, para todos terem tráfego parecido.
    entries = [checkout_request, catalog_list, auth_login, payment_charge, shipping_create,
               bank_request, stream_request, fleet_request]
    try:
        for i in range(args.iterations):
            for fn in entries:
                token = context.attach(context.Context())
                try:
                    fn()
                except Failure:
                    pass
                finally:
                    context.detach(token)
            print(f"{time.strftime('%H:%M:%S')} iteração {i + 1}/{args.iterations}", flush=True)
            time.sleep(args.interval)
    finally:
        for svc in s.values():
            svc.shutdown()


if __name__ == "__main__":
    main()
