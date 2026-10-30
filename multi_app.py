"""Simula 5 serviços que se chamam entre si e envia traces, métricas e logs via OTLP/gRPC.

checkout-demo chama auth-service, payment-gateway, shipping-service e catalog-api, então cada
requisição gera um trace distribuído. Com --chaos, de tempos em tempos um serviço sorteado fica lento
ou começa a falhar por alguns minutos.

Uso:
    .venv/bin/python multi_app.py --endpoint http://<IP_APM>:4317 [--chaos] [--incident payment-gateway:latency:5]
"""

import argparse
import logging
import random
import time

from opentelemetry import context, metrics, trace
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

SERVICES = ["checkout-demo", "catalog-api", "auth-service", "payment-gateway", "shipping-service"]
MODES = ["latency", "errors"]


class Service:
    def __init__(self, name, endpoint):
        self.name = name
        resource = Resource.create({"service.name": name, "service.version": "1.0.0",
                                    "deployment.environment": "lab", "host.name": "multi-app"})
        self.tp = TracerProvider(resource=resource)
        self.tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
        self.tracer = self.tp.get_tracer(f"{name}.tracer", "1.0.0")
        self.mp = MeterProvider(resource=resource, metric_readers=[PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=endpoint, insecure=True), export_interval_millis=10000)])
        meter = self.mp.get_meter(f"{name}.meter", "1.0.0")
        self.requests = meter.create_counter("app.requests", unit="1")
        self.duration = meter.create_histogram("app.request.duration", unit="ms")
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


incidents = {}  # serviço -> (modo, fim)


def degraded(svc, mode):
    inc = incidents.get(svc)
    return inc is not None and inc[0] == mode and time.time() < inc[1]


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
    parser.add_argument("--endpoint", required=True, help="proxy APM do Zabbix, ex.: http://<IP_APM>:4317")
    parser.add_argument("--chaos", action="store_true", help="sorteia incidentes de tempos em tempos")
    parser.add_argument("--chaos-every", type=float, default=10.0, help="minutos entre incidentes")
    parser.add_argument("--chaos-minutes", type=float, default=5.0, help="duração de cada incidente")
    parser.add_argument("--chaos-start", type=float, default=10.0, help="minutos até o primeiro incidente")
    parser.add_argument("--incident", action="append", default=[],
                        help="serviço:modo:minutos, ex.: payment-gateway:latency:5 (pode repetir)")
    parser.add_argument("--minutes", type=float, default=0, help="duração total; 0 = sem fim")
    args = parser.parse_args()

    s = {name: Service(name, args.endpoint) for name in SERVICES}
    checkout, catalog, auth, payment, shipping = (s[n] for n in SERVICES)
    for spec in args.incident:
        name, mode, minutes = spec.split(":")
        incidents[name] = (mode, time.time() + float(minutes) * 60)
        print(f"incidente manual: {name} {mode} por {minutes} min", flush=True)

    def auth_validate():
        serve(auth, "POST /validate", lambda: step(auth, "token.verify", SpanKind.INTERNAL, 0.005, 0.02,
                                                   0.01, slow=(0.4, 0.9), error_msg="token inválido"))

    def auth_login():
        def h():
            step(auth, "ldap.bind", SpanKind.CLIENT, 0.02, 0.06, 0.05, slow=(0.6, 1.2), error_msg="LDAP indisponível",
                 attrs={"server.address": "ldap.lab"})
            step(auth, "token.sign", SpanKind.INTERNAL, 0.002, 0.01)
        serve(auth, "POST /login", h)

    def catalog_list():
        def h():
            if random.random() < 0.3 or degraded("catalog-api", "latency"):
                step(catalog, "cache.get", SpanKind.CLIENT, 0.001, 0.005, attrs={"db.system": "redis"})
                step(catalog, "db.query", SpanKind.CLIENT, 0.03, 0.09, 0.02, slow=(0.5, 1.1),
                     error_msg="timeout no PostgreSQL", attrs={"db.system": "postgresql"})
            else:
                step(catalog, "cache.get", SpanKind.CLIENT, 0.001, 0.005, 0.0, error_msg="Redis indisponível",
                     attrs={"db.system": "redis"})
        serve(catalog, "GET /catalog", h)

    def payment_charge():
        def h():
            step(payment, "fraud.check", SpanKind.INTERNAL, 0.01, 0.03)
            step(payment, "bank.api", SpanKind.CLIENT, 0.05, 0.15, 0.12, slow=(0.8, 1.5),
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

    entries = [(checkout_request, 0.5), (catalog_list, 0.25), (auth_login, 0.25)]
    started = time.time()
    next_chaos = started + args.chaos_start * 60
    end = started + args.minutes * 60 if args.minutes else None
    n = 0
    try:
        while end is None or time.time() < end:
            if args.chaos and time.time() >= next_chaos:
                name, mode = random.choice(SERVICES), random.choice(MODES)
                incidents[name] = (mode, time.time() + args.chaos_minutes * 60)
                print(f"{time.strftime('%H:%M:%S')} incidente: {name} {mode} por {args.chaos_minutes:.0f} min", flush=True)
                next_chaos = time.time() + args.chaos_every * 60
            fn = random.choices([e[0] for e in entries], [e[1] for e in entries])[0]
            token = context.attach(context.Context())
            try:
                fn()
            except Failure:
                pass
            finally:
                context.detach(token)
            n += 1
            if n % 60 == 0:
                print(f"{time.strftime('%H:%M:%S')} {n} requisições enviadas", flush=True)
            time.sleep(1)
    finally:
        for svc in s.values():
            svc.shutdown()


if __name__ == "__main__":
    main()
