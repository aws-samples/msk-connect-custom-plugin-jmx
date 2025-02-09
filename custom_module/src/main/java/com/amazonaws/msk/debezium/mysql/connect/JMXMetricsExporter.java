package com.amazonaws.msk.debezium.mysql.connect;

import org.slf4j.*;
import software.amazon.awssdk.regions.*;
import software.amazon.awssdk.services.cloudwatch.*;
import software.amazon.awssdk.services.cloudwatch.model.*;

import javax.management.*;
import javax.management.remote.*;
import java.io.*;
import java.time.*;
import java.time.format.*;
import java.util.*;

import static com.amazonaws.msk.debezium.mysql.connect.Configuration.*;


public class JMXMetricsExporter extends TimerTask{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(JMXMetricsExporter.class);

	@Override
	public void run() {
		extractJMXMetrics();
	}

	private void pushCWMetric(MBeanServerConnection mbsc, ObjectName connectorMBean, String dimensionTypeValue, Set<String> metricsSet) {
		//
		Region region = Region.of(DebeziumMySqlMetricsConnector.getCWRegion());
		CloudWatchClient cw = CloudWatchClient.builder()
				.region(region)
				.build();
		// Define the metrics to send
		List<MetricDatum> metrics = new ArrayList<>();
		Dimension dimension = Dimension.builder()
				.name("DBServerName")
				.value(DebeziumMySqlMetricsConnector.getDatabaseServerName())
				.build();
		Dimension dimensionType = Dimension.builder()
				.name("Type")
				.value(dimensionTypeValue)
				.build();

		// Set an Instant object.
		String time = ZonedDateTime.now(ZoneOffset.UTC).format( DateTimeFormatter.ISO_INSTANT );
		Instant instant = Instant.parse(time);

		String[] metricsArr = new String[metricsSet.size()];


		try{
            LOGGER.info("Object Instance... {}", mbsc.getObjectInstance(connectorMBean));

			AttributeList jmxMetricList = mbsc.getAttributes(connectorMBean, (metricsSet.toArray(metricsArr)));

			for(Attribute attribute : jmxMetricList.asList()) {
                LOGGER.debug("{} {}", attribute.getName(), mbsc.getAttributes(connectorMBean, new String[]{attribute.getName()}));

				if (RegexPatternMatcher.isMatch(attribute.getName()) ){
					metrics.add(toMetricDatum(attribute.getName(),attribute.getName(),attribute.getValue(),instant,dimension,dimensionType));
				}
			}
			if (!metrics.isEmpty()){
				PutMetricDataRequest request = PutMetricDataRequest.builder()
						.namespace(DebeziumMySqlMetricsConnector.getCWNameSpace())
						.metricData(metrics).build();

				cw.putMetricData(request);
				LOGGER.info("Successfully pushed {} metrics to CloudWatch", dimensionTypeValue);
			}else {
				LOGGER.info("No CloudWatch metrics to push for  pushed {}", dimensionTypeValue);
			}


			}  catch (CloudWatchException e) {
			LOGGER.error("An error occurred while pushing the metrics to CW",e);
		}
			catch (Exception e) {
			throw new RuntimeException(e);
		}

		cw.close();
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
			pushCWMetric(mbsc,connectorMBean,"Streaming",DebeziumMetricSet.getStreamingMetrics());

			//String snapshotObjName = "debezium.mysql:type=connector-metrics,context=snapshot,server="+dbServerName;
			String snapshotObjName = String.format(SNAPSHOT_MBEAN_OBJECT_NAME_TEMPLATE, DebeziumMySqlMetricsConnector.getDatabaseServerName());
			ObjectName connectorSnapshotMBean = new ObjectName(snapshotObjName);
			pushCWMetric(mbsc,connectorSnapshotMBean,"Snapshot",DebeziumMetricSet.getSnapshotMetrics());

			//String schemaHistoryObjName = "debezium.mysql:type=connector-metrics,context=schema-history="+dbServerName;
			String schemaHistoryObjName = String.format(SCHEMA_HISTORY_MBEAN_OBJECT_NAME_TEMPLATE, DebeziumMySqlMetricsConnector.getDatabaseServerName());
			ObjectName connectorSchemaHistoryMBean = new ObjectName(schemaHistoryObjName);
			pushCWMetric(mbsc,connectorSchemaHistoryMBean,"SchemaHistory",DebeziumMetricSet.getSchemaHistoryMetrics());


			jmxConnector.close();
			
		} catch (IOException | JMException  e) {
			LOGGER.error("An error occurred while retrieving  JMXMetrics", e);
		}
	}


}
