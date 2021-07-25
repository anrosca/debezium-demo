# Debezium ETL (without T) demo

## Setup guide

### Log-based CDC with Debezium source connector and JdbcSink connector


### Start infrastructure

Go to the `src/main/resources/docker` folder and from there start all docker containers using the following command:

```bash
$ cd src/main/resources/docker
$ docker-compose up
```

### Check if Kafka connect is ready to use

```bash
$ curl http://localhost:8083 | jq
```

If kafka connect is ready, you should see something like the following:

```json
{
  "version": "6.0.1-ccs",
  "commit": "9c1fbb3db1e0d69d",
  "kafka_cluster_id": "nlrLXediT4qVVz1ZOz7Eeg"
}
```

### Check kafka connect's connector plugins

```bash
$ curl http://localhost:8083/connector-plugins/ | jq
```

You should see something like the following:

```json
[
  {
    "class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "type": "sink",
    "version": "10.2.0"
  },
  {
    "class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "type": "source",
    "version": "10.2.0"
  },
  {
    "class": "io.debezium.connector.postgresql.PostgresConnector",
    "type": "source",
    "version": "1.6.0.Final"
  },
  {
    "class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
    "type": "sink",
    "version": "6.0.1-ccs"
  },
  {
    "class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
    "type": "source",
    "version": "6.0.1-ccs"
  },
  {
    "class": "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
    "type": "source",
    "version": "1"
  },
  {
    "class": "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
    "type": "source",
    "version": "1"
  },
  {
    "class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
    "type": "source",
    "version": "1"
  }
]
```

We'll use `io.debezium.connector.postgresql.PostgresConnector` as our source connector 
    (which will stream changes from our source database to kafka) and
 `io.confluent.connect.jdbc.JdbcSinkConnector` as our sink connector 
 (which will populate our data from kafka into the destination db).

### Populate the source database with data (using Flyway)

Go to the project's root directory and execute the following command:

```bash
$ mvn flyway:migrate
```

If everything is okay, you should see something like this:

```text
[INFO] --- flyway-maven-plugin:7.11.3:migrate (default-cli) @ debezium-demo ---
[INFO] Flyway Community Edition 7.11.3 by Redgate
[INFO] Database: jdbc:postgresql://localhost:5435/crypto-profit-tracker (PostgreSQL 13.3)
[INFO] Successfully validated 2 migrations (execution time 00:00.033s)
[INFO] Creating Schema History table "public"."flyway_schema_history" ...
[INFO] Current version of schema "public": << Empty Schema >>
[INFO] Migrating schema "public" to version "20210513110100 - initial schema"
[INFO] DB: relation "investment_records" does not exist, skipping
[INFO] DB: relation "investments" does not exist, skipping
[INFO] DB: relation "users" does not exist, skipping
[INFO] DB: relation "user_roles" does not exist, skipping
[INFO] DB: table "investment_records" does not exist, skipping
[INFO] DB: table "investments" does not exist, skipping
[INFO] DB: table "users" does not exist, skipping
[INFO] Migrating schema "public" to version "20210513111200 - test data"
[INFO] Successfully applied 2 migrations to schema "public", now at version v20210513111200 (execution time 00:00.493s)
[INFO] Flyway Community Edition 7.11.3 by Redgate
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  3.320 s
[INFO] Finished at: 2021-07-25T12:58:27+03:00
[INFO] ------------------------------------------------------------------------
```

### Deploy the Debezium source connector

```bash
$ curl -XPOST -H"Content-Type: application/json" -d'
{
  "name": "cpt-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "cpt-operational-db",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "crypto-profit-tracker",
    "database.server.name": "cpt",
    "schema.whitelist": "public",
    "plugin.name": "pgoutput",
    "topic.prefix": "cpt-",
    "dialect.name": "PostgreSqlDatabaseDialect",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "transforms": "Reroute",
    "transforms.Reroute.type": "io.debezium.transforms.ByLogicalTableRouter",
    "transforms.Reroute.topic.regex": "cpt\\.public\\.(.*)",
    "transforms.Reroute.topic.replacement": "cpt_$1"
  }
}'\
 http://localhost:8083/connectors/ | jq
```

### Check the deployed connectors

To see the deployed connector names, execute the following HTTP request:

```shell
$ curl http://localhost:8083/connectors | jq
```

You should get the following response:

```json
[
  "cpt-connector"
]
```

To see the connector's status, execute the following HTTP request:

```bash
$ curl http://localhost:8083/connectors/cpt-connector/status | jq
```

And you should get the following response:

```json
{
  "name": "cpt-connector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "kafka-connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "kafka-connect:8083"
    }
  ],
  "type": "source"
}
```

If the connector doesn't have the `RUNNING` state, it means that something went wrong.

To see the topics to which the connector will stream data to, execute the following command:

```bash
$ curl http://localhost:8083/connectors/cpt-connector/topics | jq
```

And you should get the following response:

```json
{
  "cpt-connector": {
    "topics": [
      "cpt_users",
      "cpt_investments",
      "cpt_flyway_schema_history",
      "cpt_investment_records",
      "cpt_user_roles"
    ]
  }
}
```

### Deploy the JdbcSink connector

To deploy the JdbcSink connector (which will stream data from the above kafka topics to the target database), 
 execute the following HTTP request:

```bash
$ curl -XPOST -H"Content-type: application/json" -d'
{
  "name": "jdbc-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "connection.password": "postgres",
    "connection.user": "postgres",
    "connection.url": "jdbc:postgresql://cpt-reporting-db:5432/crypto-profit-tracker-reporting",
    "connection.ds.pool.size": "5",
    "dialect.name": "PostgreSqlDatabaseDialect",
    "tasks.max": "1",
    "topics": "cpt_users,cpt_user_roles,cpt_investments,cpt_investment_records",
    "insert.mode.databaselevel": "true",
    "auto.evolve": "true",
    "name": "jdbc-sink",
    "auto.create": "true",
    "transforms": "unwrap,addTopicSuffix",
    "transforms.addTopicSuffix.replacement": "$1",
    "transforms.addTopicSuffix.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.addTopicSuffix.regex": "(.*)",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "insert.mode": "upsert",
    "delete.enabled": "true",
    "pk.fields": "id",
    "pk.mode": "record_key",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "time.precision.mode": "connect"
  }
}'\
 http://localhost:8083/connectors/ | jq
 
```

Now if you'll try to get the list of all connectors, you should see the newly deployed connector, like this:

```bash
$ curl http://localhost:8083/connectors/ | jq
```

And you'll get the following response:

```json
[
  "cpt-connector",
  "jdbc-sink"
]
```

To check the JdbcSink's connector status, execute the following HTTP request:

```bash
$ curl http://localhost:8083/connectors/jdbc-sink/status | jq
```

And you should see the following response:

```json
{
  "name": "jdbc-sink",
  "connector": {
    "state": "RUNNING",
    "worker_id": "kafka-connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "kafka-connect:8083"
    }
  ],
  "type": "sink"
}
```

And if you want to see from which kafka topics the JdbcSink connector is consuming data from, execute the following HTTP request:

```bash
$ curl http://localhost:8083/connectors/jdbc-sink/topics | jq
```

You should get the following response:

```json
{
  "jdbc-sink": {
    "topics": [
      "cpt_users",
      "cpt_investments",      
      "cpt_user_roles",       
      "cpt_investment_records"
    ]
  }
}
```

### Check the destination database and enjoy!

## Polling-based CDC with JdbcSource connector and JdbcSink connector

Let's say that you're starting from scratch and you've already executed the `docker-compose up` command.
The next step would be to deploy the JdbcSink connector (which is polling-based, as opposed to Debezium connector).
Execute the following HTTP request to deploy the JdbcSink connector:

```bash
$ curl -XPOST -H"Content-type: application/json" -d'
{
  "name": "jdbc-source",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "timestamp.column.name": "updated_at",
    "connection.password": "postgres",
    "transforms.InsertTopic.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms": "addTopicSuffix,InsertTopic,InsertSourceDetails,copyFieldToKey,extractValuefromStruct,MaskField",
    "transforms.MaskField.type": "org.apache.kafka.connect.transforms.MaskField$Value",
    "transforms.MaskField.fields": "password",
    "transforms.MaskField.replacement": "********",
    "transforms.extractValuefromStruct.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
    "transforms.addTopicSuffix.replacement": "$1",
    "transforms.addTopicSuffix.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.addTopicSuffix.regex": "(.*)",
    "transforms.InsertTopic.topic.field": "messagetopic",
    "table.whitelist": "investments,investment_records,users,user_roles,foo",
    "mode": "timestamp",
    "topic.prefix": "cpt_",
    "transforms.copyFieldToKey.type": "org.apache.kafka.connect.transforms.ValueToKey",
    "dialect.name": "PostgreSqlDatabaseDialect",
    "poll.inverval.ms": "1000",
    "transforms.copyFieldToKey.fields": "id",
    "validate.non.null": "false",
    "transforms.InsertSourceDetails.static.field": "messagesource",
    "transforms.InsertSourceDetails.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "connection.user": "postgres",
    "name": "jdbc-source",
    "numeric.mapping": "best_fit",
    "transforms.extractValuefromStruct.field": "id",
    "connection.url": "jdbc:postgresql://cpt-operational-db:5432/crypto-profit-tracker",
    "transforms.InsertSourceDetails.static.value": "JDBC Source Connector cpt db",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter"
  }
}'\
 http://localhost:8083/connectors/ | jq
 
```

Check the deployed connector status:

```bash
$ curl http://localhost:8083/connectors/jdbc-sink/status | jq
```

And you should get the following response:

```json
{
  "name": "jdbc-sink",
  "connector": {
    "state": "RUNNING",
    "worker_id": "kafka-connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "kafka-connect:8083"
    }
  ],
  "type": "sink"
}
```

And check the kafka topics the connector streams data to:

```bash
$ curl http://localhost:8083/connectors/jdbc-sink/topics | jq
```

You might see the following:

```json
{
  "jdbc-sink": {
    "topics": [
      "cpt_investments"
    ]
  }
}
```

Deploy the JdbcSink connector

```bash
$ curl -XPOST -H"Content-type: application/json" -d'
{
  "name": "jdbc-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "connection.password": "postgres",
    "connection.user": "postgres",
    "connection.url": "jdbc:postgresql://cpt-reporting-db:5432/crypto-profit-tracker-reporting",
    "connection.ds.pool.size": "5",
    "dialect.name": "PostgreSqlDatabaseDialect",
    "tasks.max": "1",
    "topics": "cpt_users,cpt_user_roles,cpt_investments,cpt_investment_records",
    "insert.mode.databaselevel": "true",
    "auto.evolve": "true",
    "name": "jdbc-sink",
    "auto.create": "true",
    "transforms": "unwrap,addTopicSuffix",
    "transforms.addTopicSuffix.replacement": "$1",
    "transforms.addTopicSuffix.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.addTopicSuffix.regex": "(.*)",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "insert.mode": "upsert",
    "delete.enabled": "true",
    "pk.fields": "id",
    "pk.mode": "record_key",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "time.precision.mode": "connect"
  }
}'\
 http://localhost:8083/connectors/ | jq
 
```

### Check the destination database!
