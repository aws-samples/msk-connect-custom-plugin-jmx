## MSK Custom plugin to export JMX Metrics for Debezium MySQL connector

### Use Case
Amazon MSK Connect is a feature of Amazon MSK which allows you to run fully managed Apache Kafka Connect workloads on AWS. It simplifies the deployment, monitoring, and automatic scaling of connectors that transfer data between Apache Kafka clusters and external systems such as databases, file systems, and search indices. Amazon MSK Connect is fully compatible with Kafka Connect. It enables you to migrate your Kafka Connect applications as it is without any code modifications. MSK Connect supports Amazon MSK, Apache Kafka, and Apache Kafka compatible clusters as sources and sinks. 

MSK Connect is frequently used for Change Data Capture (CDC), a process of identifying and capturing changes made in a database and delivering those changes in real time to a downstream system. Debezium is an open-source distributed platform which provides CDC functionality and built on top of Apache Kafka. It provides a set of connectors to track and stream changes from databases to Kafka. When using these custom plugins, it becomes crucial to monitor their performance and health to ensure the seamless operation of data pipelines. 

While MSK Connect provides a range of built-in connectors for popular data sources and sinks, it currently does not support exporting JMX metrics natively. MSK Connect enables you to create custom plugins using which you can write custom code to export the JMX metrics. By exporting the JMX metrics with the custom plugin, you can integrate MSK Connect with AWS services like Amazon CloudWatch or external monitoring tools, ensuring comprehensive monitoring and observability for your connectors.
In this solution, we'll demonstrate how to build a custom module for the Debezium MySQL connector plugin to export its JMX metrics and publish them as custom metrics to Amazon CloudWatch.

### Solution Overview
Following diagram shows the workflow of using Debezium MySQL Connector as a custom plugin in MSK Connect for CDC from a Amazon Aurora MySQL compatible database (data source) to Amazon Simple Storage Service (Amazon S3) sink.

![image](https://github.com/aws-samples/msk-connect-custom-plugin-jmx/assets/65406323/12f7342e-4f14-4f27-93ee-13b862abcfc1)

1. On the producer side, MySQL binary log (binlog) is enabled to record all the operations in the order in which they are committed to the database. 
2. Debezium MySQL Connector continuously monitors the MySQL databases and captures the row-level changes by reading the MySQL bin logs and streams them as change events to Kafka topics in Amazon MSK. 
3. Amazon S3 Sink Connector reads the records from Kafka topics in Amazon MSK and deserializes the records.
4. Amazon S3 Sink Connector exports the deserialized records and stores it in S3. 

In the following sections, we will discuss the steps to build a custom module for the Debezium MySQL connector and export the JMX metrics provided by the connector and publish them as custom metrics to Amazon CloudWatch. 

The Debezium MySQL connector provides three types of metrics in addition to the built-in support for JMX metrics that Kafka, and Kafka Connect provide by default. 
  1.	**Snapshot metrics** provide information about connector operation while performing a snapshot.
  2.	**Streaming metrics** provide information about connector operation when the connector is reading the binlog.
  3.	**Schema history** metrics provide information about the status of the connector’s schema history.

In this code sample, as an example we showcase how to export variety of JMX metrics in a configurable way using connector configurations. The relevant metrics are emitted by the Debezium MySQL Connector plugin and published them as custom metrics to Amazon CloudWatch. This is achieved by creating a custom code wrapper around the Debezium MySQL Connector Plugin. e.g. The **MilliSecondsBehindDataSource** streaming metric indicates the number of milliseconds between the timestamp of the last change event and the time when the connector processes it, accounting for any clock differences between the database server and the connector's host machine.


### Architecture

![image](https://github.com/aws-samples/msk-connect-custom-plugin-jmx/assets/65406323/c0a2435d-d89a-426e-8785-1021e689e206)

The above architecture demonstrates how we build an Amazon MSK Connect Custom plugin which reports JMX metrics and pushes the metrics to CloudWatch, the high-level steps are as follows
1.	**Create a Custom Module**: Create a new Maven project that will contain your custom code to
  a.	Integrate with Debezium MySQL Connector
  b.	Create a JMX Registry and run it in the worker of the connector
  c.	Create a JMX Metrics Exporter to the JMX Registry, query the metrics and push it to CloudWatch as a custom metric
  d.	Scheduler to run the JMX Metrics Exporter at configured interval
2.	**Package and deploy** the custom module as a MSK Connect Custom Plugin
3.	**Create a connector** using the custom plugin to capture CDC from the source and stream it to the sink.

### Implementation Details
This github project is a sample implementation of the custom code wrapper, built on top of [debezium-connector-mysql-2.7.3.Final-plugin](https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/2.7.3.Final/debezium-connector-mysql-2.7.3.Final-plugin.tar.gz) that:

Creates a new Maven project with dependencies on:
1.	**Debezium MySQL Connector** to integrate with the connector plugin for  the core CDC functionalities 
2.	**Kafka Connect API**  for configuration and 
3.	**CloudWatch AWS SDK** to push the metrics to Amazon CloudWatch
 
**DebeziumMySqlMetricsConnector**: This is a custom wrapper class that integrates with the Debezium MySQL Connector by extending MySqlConnector class. This gets you the access to the connector's entry point to execute custom code.This class overrides the start method to get the configuration details, creates a JMX Registry and starts the JMX server. It then schedules the execution of the JMX metrics exporter at regular intervals 

**JMXMetricsExporter:** A custom class for connecting to the JMX Server, querying JMX metric, and converting it into a suitable format for exporting to CloudWatch. It also implements the logic for pushing the JMX metrics to Amazon CloudWatch using the CloudWatch PutMetricData API in AWS SDK for Java.

The JMXMetricsExporter provides comprehensive functionality for handling different types of Debezium metrics:

1. **Metric Types Support:**
   - Streaming metrics: Captures real-time binlog reading metrics
   - Snapshot metrics: Monitors database snapshot operations
   - Schema history metrics: Tracks schema change history status

2. **Flexible Metric Filtering:**
   - Supports include/exclude patterns for each JMX metric type using relevant connector configuration keys
   - Uses default metric sets when no filters are specified
   - Allows fine-grained control over which metrics to collect without rebuilding the plugin

3. **CloudWatch Integration:**
   - Publishes Cloudwatch metrics with "DBServerName" and "type" of metrics as dimension for identification
   - Supports custom namespace configuration
   - Implements automatic metric collection and publishing at configured intervals
   - Handles JMX connection management and metric extraction
   - Provides robust error handling and logging

### Configuration properties
This github project include some extra configuration properties that can be added to your connector configuration

**connect.jmx.port** This is local JMX port

**cloudwatch.namespace.name** AWS CloudWatch metrics custom namespace name to send your metrics 

**cloudwatch.region** the AWS CloudWatch region

**cloudwatch.debezium.streaming.metrics.include** A comma-separated list of streaming metric type that must be exported to CloudWatch as custom metrics. If left empty or skipped the property in the connector configuration, the plugin will send the default metrics defined in the project. ["NumberOfCommittedTransactions","MilliSecondsBehindSource"]
A non-empty list of property configuration takes precedence and overrides the default metrics configured for that metric type. 

Example configuration: includes 4 streaming metrics and excludes one, defaults snapshot metric values are exported as no custom configuration provided and only exclude property for schema history metrics is provided that exports all metrics except [MilliSecondsSinceLastAppliedChange].


![mixed-property-config](https://github.com/user-attachments/assets/8ff6d51b-c00f-466f-b796-878d74b8799d)



To emit *all* JMX attrubutes of a certain metric, mention the cofiguration property value as "ALL" (case insentive) 
![Debezium-metric-include-all](https://github.com/user-attachments/assets/245bb8e4-9745-4cad-bcef-af735fa47e7d)


**cloudwatch.debezium.streaming.metrics.exclude** Specify a comma-separated list of streaming metric types to exclude from being sent to CloudWatch as custom metrics. If this property is left blank or omitted, the plugin sends the project’s default metrics. When provided, all streaming metrics except those listed are published to CloudWatch. This setting also works in conjunction with the cloudwatch.debezium.streaming.metrics.include property, ensuring that excluded metrics are not sent even if they appear in the include list.

Similarly include & exclude properties for snapshot metrics type are -
**cloudwatch.debezium.snapshot.metrics.include** & 
**cloudwatch.debezium.snapshot.metrics.exclude**

Include & exclude properties for schemahistory metrics type - 
**cloudwatch.debezium.schema.history.metrics.include** & **cloudwatch.debezium.schema.history.metrics.exclude**


#### Packaging and deploying the custom plugin
You can either build this project locally and package the Maven project as a jar and include it in the debezium-connector-mysql-2.7.3.Final-plugin. Then package the updated debezium-connector-mysql-2.7.3.Final-plugin as a zip file and use it as a custom plugin in MSK Connect. Alternatively, you can use the  debezium-connector-mysql-2.7.3.zip attached as part of this github repository available under plugin/(2.7) folder.

#### Create a Connector with the custom plugin
To try this on your AWS account, you can refer to the [Getting Started](https://catalog.us-east-1.prod.workshops.aws/workshops/24d19e6d-0c60-4732-8861-343f20ef2b7f/en-US) lab in the MSK Connect workshop and follow the instructions below to create the connector:
Upload the custom-debezium-mysql-connector-plugin.zip  to msk-lab-_${ACCOUNT_ID}_-plugins-bucket/debezium path.

 ![image](https://github.com/aws-samples/msk-connect-custom-plugin-jmx/assets/65406323/77f6a786-93a9-48d7-83fc-5f95f1edcfaf)

On the Amazon MSK console there is a MSK Connect section. choose Custom plugins, then create custom plugin, browse the S3 bucket that you created above and select the custom plugin ZIP file you just uploaded.

![image](https://github.com/aws-samples/msk-connect-custom-plugin-jmx/assets/65406323/485ac0d5-f6ca-4303-b1f2-16fae7e1e3e4)

Enter custom-debezium-mysql-connector-plugin for the plugin name. Optionally, enter a description and click on **Create Custom Plugin**.

![image](https://github.com/aws-samples/msk-connect-custom-plugin-jmx/assets/65406323/12782919-fd22-4d0b-b6c2-1cce846f78e4)

After a few seconds you should see the plugin is created and the status is Active.
Customize the worker configuration for the connector by following the instructions in the [Customise worker configuration](https://catalog.us-east-1.prod.workshops.aws/workshops/24d19e6d-0c60-4732-8861-343f20ef2b7f/en-US/sourceconnectors/source-connector-setup#customise-worker-configuration) lab. 

In order for the connector to export custom metrics to cloudwatch, we need to attach cloudwatch putmetricdata permission to [mskconnectlab-AuroraConnectorIAMRole-XXXXX] role. Find this role inder IAM console and create a new inline policy using below.

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
![Screenshot 2025-05-14 at 12 17 07 PM](https://github.com/user-attachments/assets/4fdb6086-e9a4-4d78-a2da-a6b2651a5f1a)



#### Create MSK Connector

From the MSK section choose Connectors, then click **Create connector**. Choose custom-debezium-mysql-connector-plugin from the list of Custom Plugins, Click **Next**.

![image](https://github.com/aws-samples/msk-connect-custom-plugin-jmx/assets/65406323/e9585175-534f-43f9-b21d-4048cbc9939a)


Enter custom-debezium-mysql-connector in the Name textbox, and a description of your choice for the connector.

![image](https://github.com/aws-samples/msk-connect-custom-plugin-jmx/assets/65406323/ed8e79dc-2f2c-4c59-aad5-e1d1c938dccd)


Select the MSKCluster-msk-connect-lab  from the listed MSK clusters. From the Authentication Dropdown choose **IAM**

Copy the following configuration below, and paste in the connector configuration textbox.
```
connector.class=com.amazonaws.msk.debezium.mysql.connect.DebeziumMySqlMetricsConnector
tasks.max=1
include.schema.changes=true
topic.prefix=salesdb
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter=org.apache.kafka.connect.storage.StringConverter
database.user=master
database.server.id=123456
database.server.name=salesdb
database.port=3306
key.converter.schemas.enable=false
database.hostname=<--Your Aurora MySQL database endpoint-->
database.password=<--Your Database Password-->
value.converter.schemas.enable=false
database.include.list=salesdb
schema.history.internal.kafka.topic=internal.dbhistory.salesdb
schema.history.internal.kafka.bootstrap.servers=<--Your MSK Bootstrap Server Address-->

schema.history.internal.producer.sasl.mechanism=AWS_MSK_IAM
schema.history.internal.consumer.sasl.mechanism=AWS_MSK_IAM
schema.history.internal.producer.sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
schema.history.internal.consumer.sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
schema.history.internal.producer.sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
schema.history.internal.consumer.sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
schema.history.internal.consumer.security.protocol=SASL_SSL
schema.history.internal.producer.security.protocol=SASL_SSL

connect.jmx.port=7098
cloudwatch.namespace.name=MSK_Connect
cloudwatch.region=<--Your CloudWatch Region-->
cloudwatch.debezium.streaming.metrics.include=<--comma-separated streaming metric list-->
cloudwatch.debezium.streaming.metrics.exclude=<--comma-separated streaming metric list-->
cloudwatch.debezium.snapshot.metrics.include=<--comma-separated snapshot metric list-->
cloudwatch.debezium.snapshot.metrics.exclude=<--comma-separated snapshot metric list-->
cloudwatch.debezium.schema.history.metrics.include=<--comma-separated schema history metric list-->
cloudwatch.debezium.schema.history.metrics.exclude=<--comma-separated schema history metric list-->
```

Replace the <--Your Aurora MySQL database endpoint-->, <--Your Database Password-->, <--Your MSK Bootstrap Server Address-->, <--Your CloudWatch Region--> with the corresponding details from your account.

***NOTE***: Debezium metrics properties are *optional* to mention in the connector configuration. You can mix and match the propeties and metric list based on your business requirement.

![Screenshot 2025-05-14 at 12 26 17 PM](https://github.com/user-attachments/assets/c824cc4a-71e7-402c-8cda-a390b57df451)


Follow the remaining instructions from the [Create MSK Connector lab](https://catalog.us-east-1.prod.workshops.aws/workshops/24d19e6d-0c60-4732-8861-343f20ef2b7f/en-US/sourceconnectors/source-connector-setup#create-msk-connector) and create the connector. Ensure that the connector status changes to **Running**.

#### Verify the replication in the Kafka cluster and CloudWatch Metrics

Follow the instruction in the [Verify the replication in the Kafka cluster](https://catalog.us-east-1.prod.workshops.aws/workshops/24d19e6d-0c60-4732-8861-343f20ef2b7f/en-US/sourceconnectors/verify-source-connector) lab to setup a client and make changes to the source DB and verify that the changes are captured and sent to Kafka topics by the connector. 
To verify that the connector has published the JMX metrics to Amazon CloudWatch, click on Metrics → All Metrics in the CloudWatch console. Under custom namespace, you can see MSK_Connect with DBServerName as the dimension. Click on DBServerName to view the metrics.

![image](https://github.com/aws-samples/msk-connect-custom-plugin-jmx/assets/65406323/40a16dd0-7899-499d-843f-c3324e82cd4a)

Select the **MilliSecondBehindSource** metric with statistic as Average in the Graphed Metric to plot the graph. You can verify that the MilliSecondBehindSource metric value is greater than zero whenever any operation is being performed on the source database and returns to 0 during the idle time. 

![image](https://github.com/aws-samples/msk-connect-custom-plugin-jmx/assets/65406323/31087792-8aec-4c95-b97e-496b88b1f3f1)

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

