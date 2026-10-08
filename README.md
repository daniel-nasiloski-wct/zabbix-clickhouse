# zabbix-clickhouse

Schema das tabelas de telemetria OpenTelemetry (`otel_*`) usadas pelo APM do Zabbix 8.0 no ClickHouse.

O Zabbix não cria essas tabelas. O proxy grava nelas por posição de coluna, então a ordem segue `src/zabbix_proxy/apm/apm_dataset.c`.

## Requisitos

- ClickHouse 26.8 ou mais recente. Versões anteriores recusam o formato de timestamp que o proxy envia.
- Banco `zabbix` e usuário `zabbix` já criados.

## Importar

```bash
curl -LO https://raw.githubusercontent.com/daniel-nasiloski-wct/zabbix-clickhouse/main/otel_schema.sql.gz
zcat otel_schema.sql.gz | clickhouse-client --user zabbix --password '<SENHA_CH>'
```

Cria 7 tabelas em `zabbix`: `otel_traces`, `otel_logs`, `otel_metrics_gauge`, `otel_metrics_sum`, `otel_metrics_histogram`, `otel_metrics_exponential_histogram` e `otel_metrics_summary`. Pode ser executado de novo sem erro.

A retenção é de 31 dias (TTL `2678400` segundos). Para outra retenção, edite `otel_schema.sql` antes de importar.
