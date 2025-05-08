package com.amazonaws.msk.debezium.mysql.connect;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimerTask;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.amazonaws.msk.debezium.mysql.connect.Configuration.CW_DEBEZIUM_DEFAULT_SCHEMA_HISTORY_METRICS;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.CW_DEBEZIUM_DEFAULT_SNAPSHOT_METRICS;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.CW_DEBEZIUM_DEFAULT_STREAMING_METRICS;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.JMX_URL_TEMPLATE;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.SCHEMA_HISTORY_MBEAN_OBJECT_NAME_TEMPLATE;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.SNAPSHOT_MBEAN_OBJECT_NAME_TEMPLATE;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.STREAMING_MBEAN_OBJECT_NAME_TEMPLATE;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.utils.StringUtils;


public class JMXMetricsExporter extends TimerTask {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(JMXMetricsExporter.class);

    //private static Set<String> metricsSet = null;
	private Set<String> streamingMetricsSet = null;
    private Set<String> snapshotMetricsSet = null;
    private Set<String> schemaHistoryMetricsSet = null;

	@Override
	public void run() {
		LOGGER.info("JMXMetricsExporter started");
		LOGGER.info("JMXMetricsExporter connectJMXPort : {}", DebeziumMySqlMetricsConnector.getConnectJMXPort());

		if (!initializeMetricsSets()) {
            LOGGER.info("No metrics to be populated...Using default metrics");
            return;
        }
        
		extractAndPublishJMXMetrics();
    }

	private boolean initializeMetricsSets() {
		LOGGER.info("Initializing metrics sets...");
		streamingMetricsSet = populateMetricsSet("streaming",
				STREAMING_MBEAN_OBJECT_NAME_TEMPLATE,
				DebeziumMySqlMetricsConnector.getStreamingIncludeMetricsStr(),
				DebeziumMySqlMetricsConnector.getStreamingExcludeMetricsStr(),
				CW_DEBEZIUM_DEFAULT_STREAMING_METRICS);

		snapshotMetricsSet = populateMetricsSet("snapshot",
				SNAPSHOT_MBEAN_OBJECT_NAME_TEMPLATE,
				DebeziumMySqlMetricsConnector.getSnapshotIncludeMetricsStr(),
				DebeziumMySqlMetricsConnector.getSnapshotExcludeMetricsStr(),
				CW_DEBEZIUM_DEFAULT_SNAPSHOT_METRICS);

		schemaHistoryMetricsSet = populateMetricsSet("schema_history",
				SCHEMA_HISTORY_MBEAN_OBJECT_NAME_TEMPLATE,
				DebeziumMySqlMetricsConnector.getSchemaHistoryIncludeMetricsStr(),
				DebeziumMySqlMetricsConnector.getSchemaHistoryExcludeMetricsStr(),
				CW_DEBEZIUM_DEFAULT_SCHEMA_HISTORY_METRICS);

		return !(streamingMetricsSet.isEmpty()
				&& snapshotMetricsSet.isEmpty()
				&& schemaHistoryMetricsSet.isEmpty());
        
		}
    private Set<String> populateMetricsSet(
		String metricType, 
		String mbeanTemplate,
		String includeMetricsStr,
		String excludeMetricsStr,
		Set<String> defaultMetrics) {
			LOGGER.info("Populating {} metrics set", metricType);

            Set<String> metricsSet = new HashSet<>();
            boolean includeMetrics = StringUtils.isNotBlank(includeMetricsStr);
            boolean excludeMetrics = StringUtils.isNotBlank(excludeMetricsStr);
            boolean defaultMetricsFlag = !includeMetrics && !excludeMetrics;

            LOGGER.info("Populating {} metrics set", metricType);

            if (defaultMetricsFlag) {
                LOGGER.info("Setting default {} metrics", metricType);
                metricsSet = new HashSet<>(defaultMetrics);
            } else if (includeMetrics && excludeMetrics) {
                LOGGER.info("Both include and exclude metrics are set for {}", metricType);
                metricsSet.addAll(getMetricsAsList(includeMetricsStr));
                metricsSet.removeAll(getMetricsAsList(excludeMetricsStr));
            } else if (includeMetrics) {
                LOGGER.info("Include metrics is set for {}", metricType);
                metricsSet.addAll(getMetricsAsList(includeMetricsStr));
            } else if (excludeMetrics) {
                LOGGER.info("Exclude metrics is set for {}", metricType);
                Set<String> allMetricsSet = getAllAvailableMetrics(metricType, mbeanTemplate);
                metricsSet.addAll(allMetricsSet);
                metricsSet.removeAll(getMetricsAsList(excludeMetricsStr));
            }

            LOGGER.info("Populated {} MetricsSet: {}", metricType, metricsSet);
            return metricsSet;
        }

	private List<String> getMetricsAsList(String metricsStr) {
	return Arrays.asList(metricsStr.split(","));
	}

	private Set<String> getAllAvailableMetrics(String metricType, String mbeanTemplate) {
		Set<String> allMetricsSet = new HashSet<>();
		JMXServiceURL jmxUrl;

		try {
			jmxUrl = new JMXServiceURL(String.format(JMX_URL_TEMPLATE,
					DebeziumMySqlMetricsConnector.getConnectJMXPort()));
		

			try (JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxUrl, null)) {
					LOGGER.info("Connected to JMX Server Successfully for {}", metricType);

					MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();
					String objName = String.format(mbeanTemplate,
							DebeziumMySqlMetricsConnector.getDatabaseServerName());

					LOGGER.info("{} Metric Object Name: {}", metricType, objName);

					try {
						MBeanAttributeInfo[] attributeInfoArr = mbsc.getMBeanInfo(
							new ObjectName(objName)).getAttributes();

						LOGGER.info("Retrieved the attributeInfo for {}", metricType);

						for (MBeanAttributeInfo attributeInfo : attributeInfoArr) {
							allMetricsSet.add(attributeInfo.getName());
						}

						LOGGER.info("{} Attribute Names: {}", metricType, allMetricsSet);
					} 
					catch (MalformedObjectNameException | InstanceNotFoundException | IntrospectionException | ReflectionException e) {
						LOGGER.error("Error getting MBean attributes", e);
					}
				}
			}
		catch (IOException e) {
			LOGGER.error("Error connecting to JMX server", e);
		}
		LOGGER.info("All available {} metrics: {}", metricType, allMetricsSet);
        return allMetricsSet;	
	}

	
	private void extractAndPublishJMXMetrics(){
		try {
			Region region = Region.of(DebeziumMySqlMetricsConnector.getCWRegion());
			CloudWatchClient cw = CloudWatchClient.builder()
					.region(region)
					.build();
			Dimension dimension = Dimension.builder()
					.name("DBServerName")
					.value(DebeziumMySqlMetricsConnector.getDatabaseServerName())
					.build();
			
			JMXServiceURL jmxUrl = new JMXServiceURL(String.format(JMX_URL_TEMPLATE,
					DebeziumMySqlMetricsConnector.getConnectJMXPort()));
			
			
			try (JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxUrl, null)) {
				MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();
				
				// Extract streaming metrics
				if (!streamingMetricsSet.isEmpty()) {
					List<MetricDatum> metrics = new ArrayList<>();
					AttributeList streamingMetricsAttributes = extractMetricsByType(mbsc, "streaming",
							STREAMING_MBEAN_OBJECT_NAME_TEMPLATE,
							streamingMetricsSet);
					Dimension dimensionType = Dimension.builder()
							.name("Type")
							.value("streaming")
							.build();
					// Set an Instant object.
					String time = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
					Instant instant = Instant.parse(time);
					for (Attribute attr : streamingMetricsAttributes.asList()) {
						LOGGER.info("Publishing metric {} to CloudWatch", attr.getName());
						metrics.add(toMetricDatum(attr.getName(), attr.getName(), attr.getValue(), instant, dimension, dimensionType));
					}
					if (!metrics.isEmpty()) {
						PutMetricDataRequest request = PutMetricDataRequest.builder()
								.namespace(DebeziumMySqlMetricsConnector.getCWNameSpace())
								.metricData(metrics).build();

						cw.putMetricData(request);
						LOGGER.info("Successfully pushed {} metrics to CloudWatch", dimensionType);
					} else {
						LOGGER.info("No CloudWatch metrics to push for  pushed {}", dimensionType);
					}
				}
				
				// Extract snapshot metrics
				if (!snapshotMetricsSet.isEmpty()) {
					List<MetricDatum> metrics = new ArrayList<>();
					AttributeList snashotMetricsAttributes = extractMetricsByType(mbsc, "snapshot",
							SNAPSHOT_MBEAN_OBJECT_NAME_TEMPLATE,
							snapshotMetricsSet);
					Dimension dimensionType = Dimension.builder()
							.name("Type")
							.value("snapshot")
							.build();
					// Set an Instant object.
					String time = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
					Instant instant = Instant.parse(time);
					for (Attribute attr : snashotMetricsAttributes.asList()) {
						LOGGER.info("Publishing metric {} to CloudWatch", attr.getName());
						metrics.add(toMetricDatum(attr.getName(), attr.getName(), attr.getValue(), instant, dimension, dimensionType));
					}
					if (!metrics.isEmpty()) {
						PutMetricDataRequest request = PutMetricDataRequest.builder()
								.namespace(DebeziumMySqlMetricsConnector.getCWNameSpace())
								.metricData(metrics).build();

						cw.putMetricData(request);
						LOGGER.info("Successfully pushed {} metrics to CloudWatch", dimensionType);
					} else {
						LOGGER.info("No CloudWatch metrics to push for  pushed {}", dimensionType);
					}
						/* extractMetricsByType(mbsc, "snapshot",
					SNAPSHOT_MBEAN_OBJECT_NAME_TEMPLATE,
					snapshotMetricsSet); */
					}	

				// Extract schema history metrics
				if (!schemaHistoryMetricsSet.isEmpty()) {
					List<MetricDatum> metrics = new ArrayList<>();
					AttributeList schemaHistoryMetricsAttributes = extractMetricsByType(mbsc, "schema_history",
							SCHEMA_HISTORY_MBEAN_OBJECT_NAME_TEMPLATE,
							schemaHistoryMetricsSet);
					Dimension dimensionType = Dimension.builder()
							.name("Type")
							.value("schema_history")
							.build();
					// Set an Instant object.
					String time = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
					Instant instant = Instant.parse(time);
					for (Attribute attr : schemaHistoryMetricsAttributes.asList()) {
						LOGGER.info("Publishing metric {} to CloudWatch", attr.getName());
						metrics.add(toMetricDatum(attr.getName(), attr.getName(), attr.getValue(), instant, dimension, dimensionType));
					}
					
					if (!metrics.isEmpty()) {
						PutMetricDataRequest request = PutMetricDataRequest.builder()
								.namespace(DebeziumMySqlMetricsConnector.getCWNameSpace())
								.metricData(metrics).build();

						cw.putMetricData(request);
						LOGGER.info("Successfully pushed {} metrics to CloudWatch", dimensionType);
					} else {
						LOGGER.info("No CloudWatch metrics to push for  pushed {}", dimensionType);
					}
/* 					extractMetricsByType(mbsc, "schema_history",
							SCHEMA_HISTORY_MBEAN_OBJECT_NAME_TEMPLATE,
							schemaHistoryMetricsSet); */
				}
			}
			cw.close();
		} catch (Exception e) {
			LOGGER.error("Error extracting JMX metrics", e);
		}
		
	}

	private AttributeList extractMetricsByType(MBeanServerConnection mbsc,
			String metricType,
			String mbeanTemplate,
			Set<String> metricsSet) {
		AttributeList attributes = null;
		try {
			String objName = String.format(mbeanTemplate,
					DebeziumMySqlMetricsConnector.getDatabaseServerName());
			ObjectName mbean = new ObjectName(objName);

			attributes = mbsc.getAttributes(mbean,metricsSet.toArray(new String[0]));
			LOGGER.info("Extracted {} metrics: {}", metricType, attributes);
		} catch (MalformedObjectNameException | ReflectionException | InstanceNotFoundException e) {
			LOGGER.error("Error extracting {} metrics due to JMX-related issue", metricType, e);
		} catch (IOException e) {
			LOGGER.error("Error extracting {} metrics due to I/O issue", metricType, e);
		}
			
		return attributes;
							
	}
	private MetricDatum toMetricDatum(String metricName, String metricDescription , Object metricValue,Instant instant,Dimension dimension ,Dimension dimensionType){

		StandardUnit unit = metricName.toLowerCase().contains("milli") ? StandardUnit.MILLISECONDS : StandardUnit.NONE ;
		Double dblMetricValue = metricValue != null?Double.parseDouble(String.valueOf(metricValue)):0D;
		return MetricDatum.builder()
				.metricName(metricDescription)
				.unit(unit)
				.value(dblMetricValue)
				.timestamp(instant)
				.dimensions(dimension,dimensionType).build();

	}
}
