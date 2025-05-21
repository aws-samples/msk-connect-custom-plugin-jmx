## MSK Custom plugin to export JMX Metrics for MongoDB connector

### Use Case
Amazon MSK Connect is a feature of Amazon MSK which allows you to run fully managed Apache Kafka Connect workloads on AWS. It simplifies the deployment, monitoring, and automatic scaling of connectors that transfer data between Apache Kafka clusters and external systems such as databases, file systems, and search indices. Amazon MSK Connect is fully compatible with Kafka Connect. It enables you to migrate your Kafka Connect applications as it is without any code modifications. MSK Connect supports Amazon MSK, Apache Kafka, and Apache Kafka compatible clusters as sources and sinks.

Document databases like Amazon DocumentDB (with MongoDB compatibility) are increasing in usage as developers and application owners prefer the schema-less flexibility of JSON schema in modern application developments. Amazon DocumentDB is a scalable, durable, and fully-managed database service for operating mission-critical MongoDB workloads. Increasingly, customers are using Amazon MSK with Amazon DocumentDB for various use cases along with the open-source MongoDB Kafka connector to move data between Amazon MSK and Amazon DocumentDB.

Amazon DocumentDB can act as both the data sink and data source to Amazon MSK in different use cases.

#### Amazon DocumentDB as a data sink
The following are example use cases in which you can use Amazon DocumentDB as a data sink behind Amazon MSK:

- **Streaming data for live video streaming or flash sale events**: In a large video streaming or flash sale event, high volume data generated relating to viewers' reactions or a buyer's clickstream can be fed to Amazon MSK as raw data. You can further stream this data to Amazon DocumentDB for downstream processing and aggregation.
- **Streaming telemetry data from IoT devices or website hit data**: For streaming of telemetry data from Internet of Things (IoT) devices, website hit data, or meteorological data, the data can be streamed into Amazon DocumentDB using the connector and then processed (such as aggregation or min/max calculation).
- **Record replay or application recovery in Amazon DocumentDB**: In the Amazon DocumentDB cluster, rather than restoring the whole backup, the application can replay specific item-level changes from Amazon MSK to the Amazon DocumentDB collection.


While MSK Connect provides a range of built-in connectors for popular data sources and sinks, it currently does not support exporting JMX metrics natively. MSK Connect enables you to create custom plugins using which you can write custom code to export the JMX metrics. By exporting the JMX metrics with the custom plugin, you can integrate MSK Connect with AWS services like Amazon CloudWatch or external monitoring tools, ensuring comprehensive monitoring and observability for your connectors.

In this solution, we'll demonstrate how to build a custom module for the MongoDB connector plugin to export its JMX metrics and publish them as custom metrics to Amazon CloudWatch.

### Solution Overview
Following diagram shows the workflow of using MongoDB Connector as a custom plugin in MSK Connect to move the data from Amazon MSK (source) to Amazon DocumentDB (sink).


1. On the producer side, a Kafka Producer application pushes the data to a Kafka topic in the Amazon MSK cluster. 
2. MongoDB Connector reads the details from this topic and puts the details in Amazon DocumentDB. 

In the following sections, we will discuss the steps to build a custom module for the MongoDB connector and export the JMX metrics provided by the connector and publish them as custom metrics to Amazon CloudWatch.

The MongoDB Kafka Connector provides metrics for individual tasks. Tasks are classes instantiated by Kafka Connect that copy data to and from datastores and  Apache Kafka. MongoDB Kafka Connector supports two types of tasks:

A source task that copies data from a data store to Apache Kafka.

A sink task that copies data from Apache Kafka to a data store.

A sink connector configures one or more sink tasks. A source connector configures one or more source tasks.

In this code sample, as an example we showcase how to export variety of JMX metrics for a sink connector in a configurable way using connector configurations. The relevant metrics are emitted by the MongoDB Connector plugin and published as custom metrics to Amazon CloudWatch. This is achieved by creating a custom code wrapper around the MongoDB Sink Connector Plugin. For example, the **latest-kafka-time-difference-ms** metric indicates the number of milliseconds of the most recent time difference recorded between a MongoDB sink task and Kafka. This value is calculated by subtracting the current time of the connector's clock and the timestamp of the last record the task received.

### Architecture


The above architecture demonstrates how we build an Amazon MSK Connect Custom plugin which reports JMX metrics and pushes the metrics to CloudWatch, the high-level steps are as follows
1. **Create a Custom Module**: Create a new Maven project that will contain your custom code to
   a. Integrate with MongoDB Kafka Sink Connector
   b. Create a JMX Registry and run it in the worker of the connector
   c. Create a JMX Metrics Exporter to the JMX Registry, query the metrics and push it to CloudWatch as a custom metric
   d. Scheduler to run the JMX Metrics Exporter at configured interval
2. **Package and deploy** the custom module as a MSK Connect Custom Plugin
3. **Create a connector** using the custom plugin to move the data from the source and stream it to the sink.

### Implementation Details
This github project is a sample implementation of the custom code wrapper, built on top of [mongodb-kafka-connect-1.10.0-all](https://repo1.maven.org/maven2/org/mongodb/kafka/mongo-kafka-connect/1.10.0/mongo-kafka-connect-1.10.0-all.jar) that:

Creates a new Maven project with dependencies on:
1. **MongoDB Kafka Sink Connector** to integrate with the connector plugin for the core sink connector functionalities
2. **Kafka Connect API** for configuration and
3. **CloudWatch AWS SDK** to push the metrics to Amazon CloudWatch

**KafkaMongodbMetricsSinkConnector**: This is a custom wrapper class that integrates with the MongoDB Connector by extending MongoSinkConnector class. This gets you the access to the connector's entry point to execute custom code. This class overrides the start method to get the configuration details, creates a JMX Registry and starts the JMX server. It then schedules the execution of the JMX metrics exporter at regular intervals.

**JMXMetricsExporter:** A custom class for connecting to the JMX Server, querying JMX metric, and converting it into a suitable format for exporting to CloudWatch. It also implements the logic for pushing the JMX metrics to Amazon CloudWatch using the CloudWatch PutMetricData API in AWS SDK for Java.

The JMXMetricsExporter provides comprehensive functionality for handling different types of MongoDB metrics:

1. **Flexible Metric Filtering:**
   - Supports include/exclude patterns for each JMX metric type using relevant connector configuration keys
   - Uses default metric sets when no filters are specified
   - Allows fine-grained control over which metrics to collect without rebuilding the plugin

2. **CloudWatch Integration:**
   - Publishes Cloudwatch metrics with database name as dimension for identification
   - Supports custom namespace configuration
   - Implements automatic metric collection and publishing at configured intervals
   - Handles JMX connection management and metric extraction
   - Provides robust error handling and logging

### Configuration properties
This github project include some extra configuration properties that can be added to your connector configuration

**connect.jmx.port** This is local JMX port

**cloudwatch.namespace.name** AWS CloudWatch metrics custom namespace name to send your metrics

**cloudwatch.region** the AWS CloudWatch region

**cloudwatch.metrics.include** A comma-separated list of MongoDB Sink Connector metric types that must be exported to CloudWatch as custom metrics. If left empty or skipped the property in the connector configuration, the plugin will send the default metrics defined in the project. ["records","records-successful", "records-failed","latest-kafka-time-difference-ms"]
A non-empty list of property configuration takes precedence and overrides the default metrics configured for that metric type.


**cloudwatch.metrics.exclude** Specify a comma-separated list of connection metric types to exclude from being sent to CloudWatch as custom metrics. If this property is left blank or omitted, the plugin sends the project's default metrics. When provided, all metrics except those listed are published to CloudWatch. This setting also works in conjunction with the cloudwatch.metrics.include property, ensuring that excluded metrics are not sent even if they appear in the include list.

#### Packaging and deploying the custom plugin
You can either build this project locally and package the Maven project as a jar and include it in the mongo-kafka-connect-1.10.0-all. Then package the updated mongo-kafka-connect-1.10.0-all as a zip file and use it as a custom plugin in MSK Connect. Alternatively, you can use the custom-mongodb-connector-plugin.zip attached as part of this github repository available under plugin/(1.10.0) folder.

#### Create a Connector with the custom plugin
To try this on your AWS account, you can refer to the Stream data with Amazon DocumentDB, Amazon MSK Serverless, and Amazon MSK Connect (https://aws.amazon.com/blogs/database/stream-data-with-amazon-documentdb-amazon-msk-serverless-and-amazon-msk-connect/) blog and use the custom-mongodb-connector-plugin.zip instead of default plugin in the **Amazon DocumentDB as a sink section**. 

In order for the connector to export custom metrics to cloudwatch, we need to attach cloudwatch putmetricdata permission to [mskconnectlab-MongoDBConnectorIAMRole-XXXXX] role. Find this role under IAM console and create a new inline policy using below.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData"
            ],
            "Resource": "*"
        }
    ]
}
```


In addition to the properties specified in the blog, add the below properties.
```
connect.jmx.port=1098
cloudwatch.namespace.name=MSK_Connect
cloudwatch.region=<--Your CloudWatch Region-->
cloudwatch.metrics.include=<--comma-separated connection metric list-->
cloudwatch.metrics.exclude=<--comma-separated connection metric list-->
```

Replace the <--Your CloudWatch Region--> with the corresponding details from your account.


Follow the remaining instructions from the blog to create the connector and verify that the data is moved to the Document DB from the Kafka Connect topics in MSK Cluster by the connector. 

#### Verify the CloudWatch Metrics

To verify that the connector has published the JMX metrics to Amazon CloudWatch, click on Metrics → All Metrics in the CloudWatch console. Under custom namespace, you can see MSK_Connect with MongoDBServerName as the dimension. Click on MongoDBServerName to view the metrics.


Select the **ReplicationLagMilliseconds** metric with statistic as Average in the Graphed Metric to plot the graph. You can verify that the ReplicationLagMilliseconds metric value is greater than zero whenever any operation is being performed on the source database and returns to 0 during the idle time.



## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
