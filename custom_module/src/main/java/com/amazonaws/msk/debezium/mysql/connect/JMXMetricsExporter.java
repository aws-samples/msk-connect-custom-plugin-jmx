package com.amazonaws.msk.debezium.mysql.connect;

import io.debezium.connector.binlog.metrics.BinlogSnapshotChangeEventSourceMetricsMXBean;
import io.debezium.connector.binlog.metrics.BinlogStreamingChangeEventSourceMetricsMXBean;
import io.debezium.relational.history.SchemaHistoryMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

import static com.amazonaws.msk.debezium.mysql.connect.Configuration.*;


public class JMXMetricsExporter extends TimerTask{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(JMXMetricsExporter.class);

	@Override
	public void run() {
		extractJMXMetrics();
	}

	private void pushCWSchemaHistoryMetricMetric(SchemaHistoryMXBean mxBeanProxy){
		Region region = Region.of(DebeziumMySqlMetricsConnector.getCWRegion());
		CloudWatchClient cw = CloudWatchClient.builder()
				.region(region)
				.build();
		putCloudWatchMetricSnapshotData(cw, mxBeanProxy) ;
		cw.close();

	}

	private void putCloudWatchMetricSnapshotData(CloudWatchClient cw, SchemaHistoryMXBean mxBeanProxy){
		try {
			// Define the metrics to send
			List<MetricDatum> metrics = new ArrayList<>();
			Dimension dimension = Dimension.builder()
					.name("DBServerName")
					.value(DebeziumMySqlMetricsConnector.getDatabaseServerName())
					.build();
			Dimension dimensionType = Dimension.builder()
					.name("Type")
					.value("SchemaHistory")
					.build();

			// Set an Instant object.
			String time = ZonedDateTime.now(ZoneOffset.UTC).format( DateTimeFormatter.ISO_INSTANT );
			Instant instant = Instant.parse(time);

			if (RegexPatternMatcher.isMatch("ChangesApplied") ){
				MetricDatum datumChangesApplied = MetricDatum.builder()
						.metricName("ChangesApplied")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getChangesApplied())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumChangesApplied);
			}

			if (RegexPatternMatcher.isMatch("ChangesRecovered") ){
				MetricDatum datumChangesRecovered = MetricDatum.builder()
						.metricName("ChangesRecovered")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getChangesRecovered())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumChangesRecovered);
			}

			if (RegexPatternMatcher.isMatch("MilliSecondsSinceLastAppliedChange") ){
				MetricDatum datumMilliSecondsSinceLastAppliedChange = MetricDatum.builder()
						.metricName("MilliSecondsSinceLastAppliedChange")
						.unit(StandardUnit.MILLISECONDS)
						.value((double) mxBeanProxy.getMilliSecondsSinceLastAppliedChange())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumMilliSecondsSinceLastAppliedChange);
			}

			if (RegexPatternMatcher.isMatch("MilliSecondsSinceLastRecoveredChange") ){
				MetricDatum datumMilliSecondsSinceLastRecoveredChange = MetricDatum.builder()
						.metricName("MilliSecondsSinceLastRecoveredChange")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getMilliSecondsSinceLastRecoveredChange())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumMilliSecondsSinceLastRecoveredChange);
			}

			if (RegexPatternMatcher.isMatch("RecoveryStartTime") ){
				MetricDatum datumRecoveryStartTime = MetricDatum.builder()
						.metricName("RecoveryStartTime")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getRecoveryStartTime())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumRecoveryStartTime);
			}

			PutMetricDataRequest request = PutMetricDataRequest.builder()
					.namespace(DebeziumMySqlMetricsConnector.getCWNameSpace())
					.metricData(metrics).build();

			cw.putMetricData(request);
			LOGGER.info("Successfully pushed schema history metrics to CloudWatch");

		} catch (CloudWatchException e) {
			LOGGER.error("An error occurred while pushing the metrics to CW",e);
		}
	}

	private void pushCWSnapshotMetric(BinlogSnapshotChangeEventSourceMetricsMXBean mxBeanProxy){
		Region region = Region.of(DebeziumMySqlMetricsConnector.getCWRegion());
		CloudWatchClient cw = CloudWatchClient.builder()
				.region(region)
				.build();
		putCloudWatchMetricSnapshotData(cw, mxBeanProxy) ;
		cw.close();

	}
	private void putCloudWatchMetricSnapshotData(CloudWatchClient cw, BinlogSnapshotChangeEventSourceMetricsMXBean mxBeanProxy){
		try {
			// Define the metrics to send
			List<MetricDatum> metrics = new ArrayList<>();
			Dimension dimension = Dimension.builder()
					.name("DBServerName")
					.value(DebeziumMySqlMetricsConnector.getDatabaseServerName())
					.build();
			Dimension dimensionType = Dimension.builder()
					.name("Type")
					.value("Snapshot")
					.build();

			// Set an Instant object.
			String time = ZonedDateTime.now(ZoneOffset.UTC).format( DateTimeFormatter.ISO_INSTANT );
			Instant instant = Instant.parse(time);

			if (RegexPatternMatcher.isMatch("MilliSecondsSinceLastEvent") ){
				MetricDatum datumMilliSecondsSinceLastEvent = MetricDatum.builder()
						.metricName("MilliSecondsSinceLastEvent")
						.unit(StandardUnit.MILLISECONDS)
						.value((double) mxBeanProxy.getMilliSecondsSinceLastEvent())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumMilliSecondsSinceLastEvent);
			}

			if (RegexPatternMatcher.isMatch("NumberOfErroneousEvents") ){
				MetricDatum datumNumberOfErroneousEvents = MetricDatum.builder()
						.metricName("NumberOfErroneousEvents")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getNumberOfErroneousEvents())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumNumberOfErroneousEvents);
			}


			if (RegexPatternMatcher.isMatch("NumberOfEventsFiltered") ){
				MetricDatum datumNumberOfEventsFiltered = MetricDatum.builder()
						.metricName("NumberOfEventsFiltered")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getNumberOfEventsFiltered())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumNumberOfEventsFiltered);
			}

			if (RegexPatternMatcher.isMatch("TotalNumberOfCreateEventsSeen") ){
				MetricDatum datumTotalNumberOfCreateEventsSeen = MetricDatum.builder()
						.metricName("TotalNumberOfCreateEventsSeen")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getTotalNumberOfCreateEventsSeen())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumTotalNumberOfCreateEventsSeen);
			}

			if (RegexPatternMatcher.isMatch("TotalNumberOfDeleteEventsSeen") ){
				MetricDatum datumTotalNumberOfDeleteEventsSeen = MetricDatum.builder()
						.metricName("TotalNumberOfDeleteEventsSeen")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getTotalNumberOfDeleteEventsSeen())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumTotalNumberOfDeleteEventsSeen);
			}

			if (RegexPatternMatcher.isMatch("RecoveryStartTime") ){
				MetricDatum datumTotalNumberOfUpdateEventsSeen = MetricDatum.builder()
						.metricName("RecoveryStartTime")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getTotalNumberOfUpdateEventsSeen())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumTotalNumberOfUpdateEventsSeen);
			}

			if (RegexPatternMatcher.isMatch("TotalNumberOfEventsSeen") ){
				MetricDatum datumTotalNumberOfEventsSeen = MetricDatum.builder()
						.metricName("TotalNumberOfEventsSeen")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getTotalNumberOfEventsSeen())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumTotalNumberOfEventsSeen);
			}

			if (RegexPatternMatcher.isMatch("CurrentQueueSizeInBytes") ){
				MetricDatum datumCurrentQueueSizeInBytes = MetricDatum.builder()
						.metricName("CurrentQueueSizeInBytes")
						.unit(StandardUnit.BYTES)
						.value((double) mxBeanProxy.getCurrentQueueSizeInBytes())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumCurrentQueueSizeInBytes);
			}

			if (RegexPatternMatcher.isMatch("MaxQueueSizeInBytes") ){
				MetricDatum datumMaxQueueSizeInBytes = MetricDatum.builder()
						.metricName("MaxQueueSizeInBytes")
						.unit(StandardUnit.BYTES)
						.value((double) mxBeanProxy.getMaxQueueSizeInBytes())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumMaxQueueSizeInBytes);
			}


			if (RegexPatternMatcher.isMatch("QueueRemainingCapacity") ){
				MetricDatum datumQueueRemainingCapacity = MetricDatum.builder()
						.metricName("QueueRemainingCapacity")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getQueueRemainingCapacity())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumQueueRemainingCapacity);
			}

			if (RegexPatternMatcher.isMatch("QueueTotalCapacity") ){
				MetricDatum datumQueueTotalCapacity = MetricDatum.builder()
						.metricName("QueueTotalCapacity")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getQueueTotalCapacity())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumQueueTotalCapacity);
			}


			if (RegexPatternMatcher.isMatch("RemainingTableCount") ){
				MetricDatum datumRemainingTableCount = MetricDatum.builder()
						.metricName("RemainingTableCount")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getRemainingTableCount())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumRemainingTableCount);
			}

			if (RegexPatternMatcher.isMatch("SnapshotDurationInSeconds") ){
				MetricDatum datumSnapshotDurationInSeconds = MetricDatum.builder()
						.metricName("SnapshotDurationInSeconds")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getSnapshotDurationInSeconds())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumSnapshotDurationInSeconds);
			}

			if (RegexPatternMatcher.isMatch("SnapshotPausedDurationInSeconds") ){
				MetricDatum datumSnapshotPausedDurationInSeconds = MetricDatum.builder()
						.metricName("SnapshotPausedDurationInSeconds")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getSnapshotPausedDurationInSeconds())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumSnapshotPausedDurationInSeconds);
			}

			if (RegexPatternMatcher.isMatch("TotalTableCount") ){
				MetricDatum datumTotalTableCount = MetricDatum.builder()
						.metricName("TotalTableCount")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getTotalTableCount())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumTotalTableCount);
			}


			PutMetricDataRequest request = PutMetricDataRequest.builder()
					.namespace(DebeziumMySqlMetricsConnector.getCWNameSpace())
					.metricData(metrics).build();

			cw.putMetricData(request);
			LOGGER.info("Successfully pushed snapshot metrics to CloudWatch");

		} catch (CloudWatchException e) {
			LOGGER.error("An error occurred while pushing the metrics to CW",e);
		}
	}

	private void pushCWStreamingMetric(BinlogStreamingChangeEventSourceMetricsMXBean mxBeanProxy) {
		Region region = Region.of(DebeziumMySqlMetricsConnector.getCWRegion());
        CloudWatchClient cw = CloudWatchClient.builder()
            .region(region)
            .build();
		putCloudWatchMetricStreamingData(cw, mxBeanProxy) ;
        cw.close();
	}

	private void putCloudWatchMetricStreamingData(CloudWatchClient cw, BinlogStreamingChangeEventSourceMetricsMXBean mxBeanProxy) {
		try {
			// Define the metrics to send
			List<MetricDatum> metrics = new ArrayList<>();
			Dimension dimension = Dimension.builder()
					.name("DBServerName")
					.value(DebeziumMySqlMetricsConnector.getDatabaseServerName())
					.build();
			Dimension dimensionType = Dimension.builder()
					.name("Type")
					.value("Streaming")
					.build();

			// Set an Instant object.
			String time = ZonedDateTime.now(ZoneOffset.UTC).format( DateTimeFormatter.ISO_INSTANT );
			Instant instant = Instant.parse(time);

			if (RegexPatternMatcher.isMatch("MilliSecondsBehindSource") ){
				MetricDatum datumMilliSecondsBehindSource = MetricDatum.builder()
						.metricName("MilliSecondsBehindSource")
						.unit(StandardUnit.MILLISECONDS)
						.value((double) mxBeanProxy.getMilliSecondsBehindSource())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumMilliSecondsBehindSource);
			}

			if (RegexPatternMatcher.isMatch("MilliSecondsSinceLastEvent") ){
				MetricDatum datumMilliSecondsSinceLastEvent = MetricDatum.builder()
						.metricName("MilliSecondsSinceLastEvent")
						.unit(StandardUnit.MILLISECONDS)
						.value((double) mxBeanProxy.getMilliSecondsSinceLastEvent())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumMilliSecondsSinceLastEvent);
			}

			if (RegexPatternMatcher.isMatch("NumberOfErroneousEvents") ){
				MetricDatum datumNumberOfErroneousEvents = MetricDatum.builder()
						.metricName("NumberOfErroneousEvents")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getNumberOfErroneousEvents())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumNumberOfErroneousEvents);
			}

			if (RegexPatternMatcher.isMatch("NumberOfEventsFiltered") ){
				MetricDatum datumNumberOfEventsFiltered = MetricDatum.builder()
						.metricName("NumberOfEventsFiltered")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getNumberOfEventsFiltered())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumNumberOfEventsFiltered);
			}

			if (RegexPatternMatcher.isMatch("BinlogPosition") ){
				MetricDatum datumBinlogPosition = MetricDatum.builder()
						.metricName("BinlogPosition")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getBinlogPosition())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumBinlogPosition);
			}

			if (RegexPatternMatcher.isMatch("NumberOfLargeTransactions") ){
				MetricDatum datumNumberOfLargeTransactions = MetricDatum.builder()
						.metricName("NumberOfLargeTransactions")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getNumberOfLargeTransactions())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumNumberOfLargeTransactions);
			}

			if (RegexPatternMatcher.isMatch("TotalNumberOfCreateEventsSeen") ){
				MetricDatum datumTotalNumberOfCreateEventsSeen = MetricDatum.builder()
						.metricName("TotalNumberOfCreateEventsSeen")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getTotalNumberOfCreateEventsSeen())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumTotalNumberOfCreateEventsSeen);
			}

			if (RegexPatternMatcher.isMatch("TotalNumberOfDeleteEventsSeen") ){
				MetricDatum datumTotalNumberOfDeleteEventsSeen = MetricDatum.builder()
						.metricName("TotalNumberOfDeleteEventsSeen")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getTotalNumberOfDeleteEventsSeen())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumTotalNumberOfDeleteEventsSeen);
			}

			if (RegexPatternMatcher.isMatch("TotalNumberOfUpdateEventsSeen") ){
				MetricDatum datumTotalNumberOfUpdateEventsSeen = MetricDatum.builder()
						.metricName("TotalNumberOfUpdateEventsSeen")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getTotalNumberOfUpdateEventsSeen())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumTotalNumberOfUpdateEventsSeen);
			}

			if (RegexPatternMatcher.isMatch("TotalNumberOfEventsSeen") ){
				MetricDatum datumTotalNumberOfEventsSeen = MetricDatum.builder()
						.metricName("TotalNumberOfEventsSeen")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getTotalNumberOfEventsSeen())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumTotalNumberOfEventsSeen);

			}

			if (RegexPatternMatcher.isMatch("NumberOfCommittedTransactions") ){
				MetricDatum datumNumberOfCommittedTransactions = MetricDatum.builder()
						.metricName("NumberOfCommittedTransactions")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getNumberOfCommittedTransactions())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumNumberOfCommittedTransactions);
			}

			if (RegexPatternMatcher.isMatch("CurrentQueueSizeInBytes") ){
				MetricDatum datumCurrentQueueSizeInBytes = MetricDatum.builder()
						.metricName("CurrentQueueSizeInBytes")
						.unit(StandardUnit.BYTES)
						.value((double) mxBeanProxy.getCurrentQueueSizeInBytes())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumCurrentQueueSizeInBytes);
			}

			if (RegexPatternMatcher.isMatch("MaxQueueSizeInBytes") ){
				MetricDatum datumMaxQueueSizeInBytes = MetricDatum.builder()
						.metricName("MaxQueueSizeInBytes")
						.unit(StandardUnit.BYTES)
						.value((double) mxBeanProxy.getMaxQueueSizeInBytes())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumMaxQueueSizeInBytes);
			}

			if (RegexPatternMatcher.isMatch("QueueRemainingCapacity") ){
				MetricDatum datumQueueRemainingCapacity = MetricDatum.builder()
						.metricName("QueueRemainingCapacity")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getQueueRemainingCapacity())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumQueueRemainingCapacity);
			}

			if (RegexPatternMatcher.isMatch("QueueTotalCapacity") ){
				MetricDatum datumQueueTotalCapacity = MetricDatum.builder()
						.metricName("QueueTotalCapacity")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getQueueTotalCapacity())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumQueueTotalCapacity);
			}


			if (RegexPatternMatcher.isMatch("NumberOfDisconnects") ){
				MetricDatum datumNumberOfDisconnects = MetricDatum.builder()
						.metricName("NumberOfDisconnects")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getNumberOfDisconnects())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumNumberOfDisconnects);
			}

			if (RegexPatternMatcher.isMatch("NumberOfNotWellFormedTransactions") ){
				MetricDatum datumNumberOfNotWellFormedTransactions = MetricDatum.builder()
						.metricName("NumberOfNotWellFormedTransactions")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getNumberOfNotWellFormedTransactions())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumNumberOfNotWellFormedTransactions);
			}

			if (RegexPatternMatcher.isMatch("NumberOfRolledBackTransactions") ){
				MetricDatum datumNumberOfRolledBackTransactions = MetricDatum.builder()
						.metricName("NumberOfRolledBackTransactions")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getNumberOfRolledBackTransactions())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumNumberOfRolledBackTransactions);
			}

			if (RegexPatternMatcher.isMatch("NumberOfSkippedEvents") ){
				MetricDatum datumNumberOfSkippedEvents = MetricDatum.builder()
						.metricName("NumberOfSkippedEvents")
						.unit(StandardUnit.NONE)
						.value((double) mxBeanProxy.getNumberOfSkippedEvents())
						.timestamp(instant)
						.dimensions(dimension,dimensionType).build();

				metrics.add(datumNumberOfSkippedEvents);
			}

			PutMetricDataRequest request = PutMetricDataRequest.builder()
					.namespace(DebeziumMySqlMetricsConnector.getCWNameSpace())
					.metricData(metrics).build();

			cw.putMetricData(request);
			LOGGER.info("Successfully pushed streaming metrics to CloudWatch");

		} catch (CloudWatchException e) {
			LOGGER.error("An error occurred while pushing the metrics to CW",e);
		}

	}

	private void extractJMXMetrics() {
		try {
			//JMXServiceURL jmxUrl = new JMXServiceURL("service:jmx:rmi://localhost/jndi/rmi://localhost:"+connectJMXPort+"/jmxrmi");
			JMXServiceURL jmxUrl = new JMXServiceURL(String.format(JMX_URL_TEMPLATE,DebeziumMySqlMetricsConnector.getConnectJMXPort()));
			LOGGER.info("JMX URL : {}", jmxUrl);
			JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxUrl, null);
			jmxConnector.connect();
			LOGGER.info("Connected to JMX Server Successfully");
			MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();

			//String streamingObjName = "debezium.mysql:type=connector-metrics,context=streaming,server="+dbServerName;
			String streamingObjName = String.format(STREAMING_MBEAN_OBJECT_NAME_TEMPLATE, DebeziumMySqlMetricsConnector.getDatabaseServerName());
			ObjectName connectorMBean = new ObjectName(streamingObjName);
			// streaming metrics
			BinlogStreamingChangeEventSourceMetricsMXBean mxBeanProxy = JMX.newMXBeanProxy(mbsc, connectorMBean, BinlogStreamingChangeEventSourceMetricsMXBean.class);
			pushCWStreamingMetric(mxBeanProxy);

			//String snapshotObjName = "debezium.mysql:type=connector-metrics,context=snapshot,server="+dbServerName;
			String snapshotObjName = String.format(SNAPSHOT_MBEAN_OBJECT_NAME_TEMPLATE, DebeziumMySqlMetricsConnector.getDatabaseServerName());
			ObjectName connectorSnapshotMBean = new ObjectName(snapshotObjName);
			// snapshot metrics
			BinlogSnapshotChangeEventSourceMetricsMXBean mxSnapshotBeanProxy = JMX.newMXBeanProxy(mbsc, connectorSnapshotMBean, BinlogSnapshotChangeEventSourceMetricsMXBean.class);
			pushCWSnapshotMetric(mxSnapshotBeanProxy);

			//String schemaHistoryObjName = "debezium.mysql:type=connector-metrics,context=schema-history="+dbServerName;
			String schemaHistoryObjName = String.format(SCHEMA_HISTORY_MBEAN_OBJECT_NAME_TEMPLATE, DebeziumMySqlMetricsConnector.getDatabaseServerName());
			ObjectName connectorSchemaHistoryMBean = new ObjectName(schemaHistoryObjName);
			// schema history metrics
			SchemaHistoryMXBean mxSchemaHistoryBeanProxy = JMX.newMXBeanProxy(mbsc, connectorSchemaHistoryMBean, SchemaHistoryMXBean.class);
			pushCWSchemaHistoryMetricMetric(mxSchemaHistoryBeanProxy);

			jmxConnector.close();
			
		} catch (IOException | MalformedObjectNameException  e) {
			LOGGER.error("An error occurred while retrieving  BinlogStreamingChangeEventSourceMetricsMXBean", e);
		}
	}


}
