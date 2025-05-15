
## Why This Plugin?
Amazon MSK Connect simplifies the deployment, monitoring, and automatic scaling of connectors that transfer data between Apache Kafka clusters and external systems such as databases, file systems, and search indices, It is fully compatible with Kafka Connect and supports Amazon MSK, Apache Kafka and Apache Kafka compatible clusters. MSK Connect uses custom plugins as the container to implement custom business logic.

Custom connect plugins use JMX to expose runtime metrics. While MSK Connect sends a set of connector metrics to Amazon CloudWatch, it currently does not support exporting the JMX metrics emitted by the connector plugins natively. These metrics can be exported by modifying the custom connect plugin code directly.

# AWS MSK Connector Metrics Exporter for Debezium and MongoDB
A custom connector that exports Debezium MySQL and MongoDB metrics to Amazon CloudWatch, enabling real-time monitoring of database change data capture (CDC) operations in AWS Managed Streaming for Apache Kafka (MSK) environments.

While Debezium provides powerful CDC capabilities, monitoring its operations in AWS MSK environments presents several challenges:

1. Limited Visibility: Standard Debezium connectors expose metrics through JMX, but these aren't readily accessible in managed Kafka environments
2. Monitoring Gap: Without direct access to JMX metrics, it's difficult to track critical metrics like replication lag and CDC progress
3. Integration Needs: AWS MSK users need CloudWatch integration for consistent monitoring across their AWS infrastructure

This project provides specialized connectors that extend Debezium MySQL and MongoDB Kafka connectors to capture operational metrics and publish them to Amazon CloudWatch. It supports multiple versions of Debezium (2.5.2, 2.7.3) and MongoDB (1.10.0), allowing users to monitor the health and performance of their CDC pipelines through CloudWatch metrics.

The connectors capture essential debezium JMX metrics like streaming metrics, snapshot metrics, and schema history metrics, providing comprehensive visibility into the CDC process.


## Repository Structure
```
.
├── custom_module/                      # Root directory for all connector modules
    ├── debezium/                      # Debezium connector implementations
    │   ├── 2.5.2/                     # Debezium 2.5.2 implementation
    │   │   └── src/                   # Source code for 2.5.2 version
    │   └── 2.7.3/                     # Debezium 2.7.3 implementation
    │       └── src/                   # Source code for 2.7.3 version
    └── mongodb/                       # MongoDB connector implementation
        └── 1.10.0/                    # MongoDB 1.10.0 implementation
            └── src/                   # Source code for MongoDB connector
```

## Usage Instructions
### Prerequisites
- Java Development Kit (JDK) 11 or later
- Apache Maven 3.6.0 or later
- AWS Account with CloudWatch access
- Apache Kafka Connect runtime environment
- Access to AWS MSK cluster
- Appropriate IAM permissions for CloudWatch metrics publishing

### Installation
1. OptionA : Download Pre-packaged Plugins
2. OptionB : Rebuild the plugin with a new custom JAR with your custom logic

### OptionA: Quick Start

1. Download Pre-packaged Plugins

Download the required connector plugin(s) directly from the releases (available as zip files):

```bash
# For Debezium 2.7.3
custom_module/debezium/2.7.3/target/msk-debezium-mysql-metrics-connector-0.0.3-SNAPSHOT.jar
# For Debezium 2.5.2
custom_module/debezium/2.5.2/target/msk-debezium-mysql-metrics-connector-0.0.1-SNAPSHOT.jar
```
2. Upload them to S3 and configure your MSK connector pointing to the plugin uploaded to S3 with relevant MSK connector configuration properties.

### OptionB: Build your own JAR

1. Clone the repository:
```bash
git clone [repository-url]
cd custom_module
```

2. Build the desired connector version:

For Debezium 2.7.3:
```bash
cd debezium/2.7.3
mvn clean package
```

For MongoDB 1.10.0:
```bash
cd mongodb/1.10.0
mvn clean package
```

3. Deploy the generated JAR file to your Kafka Connect plugins directory and convert it to zip file.

----

Configure the respective connectors with required properties:

For Debezium MySQL:
```json
{
    "connector.class": "com.amazonaws.msk.debezium.mysql.connect.DebeziumMySqlMetricsConnector",
    "database.server.name": "my-mysql-server",
    "database.hostname": "mysql-host",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz-password",
    "connect.jmx.port": "9999",
    "cw.namespace": "MyDebeziumMetrics",
    "cw.region": "us-west-2"
}
```

For MongoDB:
```json
{
    "connector.class": "com.amazonaws.msk.kafka.mongodb.connect.KafkaMongodbMetricsSinkConnector",
    "connection.uri": "mongodb://mongodb-host:27017",
    "database": "mydb",
    "connect.jmx.port": "9999",
    "cw.namespace": "MyMongoMetrics",
    "cw.region": "us-west-2"
}
```

For detailed deployment instructions and configuration options, please refer to the version-specific README files:

- Debezium 2.5.2: [custom_module/debezium/2.5.2/README.md](custom_module/debezium/2.5.2/README.md)
- Debezium 2.7.3: [custom_module/debezium/2.7.3/README.md](custom_module/debezium/2.7.3/README.md)
- MongoDB 1.10.0: [custom_module/mongodb/1.10.0/README.md](custom_module/mongodb/1.10.0/README.md)

### More Detailed Examples

Configuring metric filters for Debezium 2.7.3:
```json
{
    "cw.debezium.stream.metrics.include": "MilliSecondsBehindSource,NumberOfCommittedTransactions",
    "cw.debezium.snapshot.metrics.include": "RemainingTableCount,TotalTableCount",
    "cw.debezium.schema.history.metrics.include": "LastDatabaseOffset,LastProcessedTimestamp"
}
```

### Troubleshooting

Common issues:

1. JMX Connection Failures:
```
Error: Unable to connect to JMX server
Solution: 
- Verify JMX port is available and not blocked by firewall
- Check JMX URL format in logs
- Ensure proper permissions for JMX operations
```

2. CloudWatch Metrics Not Appearing:
```
- Verify IAM permissions for CloudWatch
- Check CloudWatch namespace configuration
- Ensure metrics are being collected (check connector logs)
- Verify region configuration matches your CloudWatch region
```

## Data Flow

The connector captures metrics from Debezium and MongoDB connectors through JMX, processes them, and publishes to CloudWatch. This enables real-time monitoring of CDC operations.

```ascii
Source DB (MySQL/MongoDB) --> Debezium/MongoDB Connector --> JMX Metrics Collection --> CloudWatch Metrics
                                         |
                                         v
                                    Kafka Connect
                                    (MSK Cluster)
```

Key component interactions:
1. Connector initializes JMX server on specified port
2. JMXMetricsExporter runs on scheduled intervals
3. Metrics are collected from MBeans
4. CloudWatch client publishes metrics with configured dimensions
5. Metrics are organized by database/server name in CloudWatch