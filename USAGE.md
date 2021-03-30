# `kadb_fdw` usage: Sample SQL scripts

## A `FOREIGN TABLE` for data in AVRO format
```sql
DROP SERVER IF EXISTS ka_server CASCADE;
CREATE SERVER ka_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers 'localhost:9092'
);

CREATE FOREIGN TABLE ka_table(
    i INT,
    t TEXT
)
SERVER ka_server
OPTIONS (
    format 'avro',
    k_topic 'my_topic',
    k_consumer_group 'my_consumer_group',
    k_seg_batch '5',
    k_timeout_ms '1000'
);
```

## A `FOREIGN TABLE` for data in CSV format
```sql
DROP SERVER IF EXISTS ka_server CASCADE;
CREATE SERVER ka_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers 'localhost:9092'
);

CREATE FOREIGN TABLE ka_table(
    i INT,
    t TEXT
)
SERVER ka_server
OPTIONS (
    format 'csv',
    k_topic 'my_topic',
    k_consumer_group 'my_consumer_group',
    k_seg_batch '5',
    k_timeout_ms '1000'
);
```

## A `FOREIGN TABLE` with Kerberos authentication
```sql
DROP SERVER IF EXISTS ka_kerberized_server CASCADE;
CREATE SERVER ka_kerberized_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers 'ke-kafka-sasl.ru-central1.internal:9092',
    k_security_protocol 'sasl_plaintext',
    kerberos_keytab '/root/adbkafka.service.keytab',
    kerberos_principal 'adbkafka'
);

CREATE FOREIGN TABLE ka_table(
    i INT,
    t TEXT
)
SERVER ka_server
OPTIONS (
    format 'avro',
    k_topic 'my_topic',
    k_consumer_group 'my_consumer_group',
    k_seg_batch '5',
    k_timeout_ms '1000'
);
```
