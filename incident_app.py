"""Simula um incidente no serviço checkout-demo: latência alta, erros ou os dois.

Envia traces, métricas e logs via OTLP/gRPC para o proxy APM do Zabbix, como o test_app.py,
mas com o pagamento lento (0,6 a 1,2 s) e/ou recusado em 80% das requisições.

Uso:
    .venv/bin/python incident_app.py --endpoint http://<IP_APM>:4317 [--mode both] [--minutes 5] [--interval 1]
"""

import argparse
import logging
import random
import time

from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
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
from opentelemetry.trace import Status, StatusCode

SERVICE_NAME = "checkout-demo"


def setup(endpoint: str):
    resource = Resource.create({
        "service.name": SERVICE_NAME,
        "service.version": "1.0.0",
        "deployment.environment": "lab",
        "host.name": "incident-app",
    })

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
    trace.set_tracer_provider(tracer_provider)

    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=endpoint, insecure=True), export_interval_millis=5000)],
    )
    metrics.set_meter_provider(meter_provider)

    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter(endpoint=endpoint, insecure=True)))
    set_logger_provider(logger_provider)

    logger = logging.getLogger(SERVICE_NAME)
    logger.setLevel(logging.INFO)
    logger.addHandler(LoggingHandler(level=logging.INFO, logger_provider=logger_provider))
    logger.addHandler(logging.StreamHandler())

    return tracer_provider, meter_provider, logger_provider, logger


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True, help="proxy APM do Zabbix, ex.: http://<IP_APM>:4317")
    parser.add_argument("--mode", choices=["latency", "errors", "both"], default="both",
                        help="latency: pagamento lento; errors: pagamento recusado; both: os dois")
    parser.add_argument("--minutes", type=float, default=5.0, help="duração do incidente")
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()

    slow = args.mode in ("latency", "both")
    error_rate = 0.8 if args.mode in ("errors", "both") else 0.15

    tracer_provider, meter_provider, logger_provider, logger = setup(args.endpoint)
    tracer = trace.get_tracer("checkout-demo.tracer", "1.0.0")
    meter = metrics.get_meter("checkout-demo.meter", "1.0.0")
    requests_total = meter.create_counter("app.requests", unit="1", description="Total de requisições")
    request_duration = meter.create_histogram("app.request.duration", unit="ms", description="Duração das requisições")

    logger.warning("Incidente iniciado: modo %s por %.0f minutos", args.mode, args.minutes)
    end = time.time() + args.minutes * 60
    i = 0
    while time.time() < end:
        i += 1
        route = "/checkout"
        failed = random.random() < error_rate

        with tracer.start_as_current_span(f"GET {route}", kind=trace.SpanKind.SERVER) as span:
            start = time.perf_counter()
            span.set_attribute("http.request.method", "GET")
            span.set_attribute("url.path", route)

            with tracer.start_as_current_span("db.query", kind=trace.SpanKind.CLIENT) as db_span:
                db_span.set_attribute("db.system", "mysql")
                db_span.set_attribute("db.statement", "SELECT * FROM orders WHERE id = ?")
                time.sleep(random.uniform(0.01, 0.08))

            with tracer.start_as_current_span("payment.authorize", kind=trace.SpanKind.CLIENT) as pay_span:
                time.sleep(random.uniform(0.6, 1.2) if slow else random.uniform(0.02, 0.15))
                if failed:
                    pay_span.add_event("payment.declined", {"reason": "gateway_timeout"})
                    pay_span.set_status(Status(StatusCode.ERROR, "payment gateway timeout"))

            status = 500 if failed else 200
            span.set_attribute("http.response.status_code", status)
            if failed:
                span.set_status(Status(StatusCode.ERROR, "checkout failed"))
                logger.error("Falha no checkout da requisição %d: gateway de pagamento não respondeu", i)
            else:
                logger.info("Requisição %d em %s concluída com sucesso", i, route)

            elapsed_ms = (time.perf_counter() - start) * 1000
            attrs = {"http.route": route, "http.response.status_code": status}
            requests_total.add(1, attrs)
            request_duration.record(elapsed_ms, attrs)
            if elapsed_ms > 150:
                logger.warning("Requisição %d lenta: %.1f ms", i, elapsed_ms)

        time.sleep(args.interval)

    logger.warning("Incidente encerrado após %d requisições", i)
    tracer_provider.force_flush()
    meter_provider.force_flush()
    logger_provider.force_flush()
    tracer_provider.shutdown()
    meter_provider.shutdown()
    logger_provider.shutdown()


if __name__ == "__main__":
    main()
