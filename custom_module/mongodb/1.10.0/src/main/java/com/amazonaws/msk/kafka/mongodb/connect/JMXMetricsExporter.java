package com.amazonaws.msk.kafka.mongodb.connect;

import static com.amazonaws.msk.kafka.mongodb.connect.Configuration.CW_DEFAULT_SINK_TASK_METRICS;
import static com.amazonaws.msk.kafka.mongodb.connect.Configuration.JMX_URL_TEMPLATE;
import static com.amazonaws.msk.kafka.mongodb.connect.Configuration.SINK_TASK_METRICS_MBEAN_OBJECT_NAME_TEMPLATE;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.utils.StringUtils;

public class JMXMetricsExporter extends TimerTask{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(JMXMetricsExporter.class);
	
	private static Set<String> metricsSet = null;

	@Override
	public void run() {
		
		LOGGER.info("BEGIN::JMXMetricsExporter::run");
		
		if(metricsSet == null) {
			populateMetricsSet();
		}
		
		if(metricsSet.isEmpty()) {
			LOGGER.info("No metrics to be populated...");
			return;
		}
		
		Region region = Region.of(KafkaMongodbMetricsSinkConnector.getCWRegion());
        CloudWatchClient cw = CloudWatchClient.builder()
            .region(region)
            .build();
        Dimension dimension = Dimension.builder()
                .name("database")
                .value(KafkaMongodbMetricsSinkConnector.getDatabase())
                .build();
        
		AttributeList metricsAttributes = getJMXMetrics();
		for(Attribute attribute : metricsAttributes.asList()) {
			putMetData(cw,dimension, attribute.getName(), attribute.getValue());
		}
		
		cw.close();
		
		LOGGER.info("END::JMXMetricsExporter::run");
	}


	private void populateMetricsSet() {
		boolean includeMetrics = StringUtils.isNotBlank(KafkaMongodbMetricsSinkConnector.getIncludeMetricsStr());
		boolean excludeMetrics = StringUtils.isNotBlank(KafkaMongodbMetricsSinkConnector.getExcludeMetricsStr());
		boolean defaultMetrics = !includeMetrics && !excludeMetrics;
		metricsSet = new HashSet<String>();
		if(defaultMetrics) {
			LOGGER.info("Setting default metrics");
			metricsSet = CW_DEFAULT_SINK_TASK_METRICS;
		}else if(includeMetrics && excludeMetrics){
			LOGGER.info("Both includeMetrics and excludeMetrics are set in the configuration");
			metricsSet.addAll(getMetricsAsList(KafkaMongodbMetricsSinkConnector.getIncludeMetricsStr()));
			metricsSet.removeAll(getMetricsAsList(KafkaMongodbMetricsSinkConnector.getExcludeMetricsStr()));
		}else if(includeMetrics) {
			LOGGER.info("Include metrics is set in the configuration");
			metricsSet.addAll(getMetricsAsList(KafkaMongodbMetricsSinkConnector.getIncludeMetricsStr()));
		}else if(excludeMetrics) {
			LOGGER.info("Exclude metrics is set in the configuration");
			Set<String> allMetricsSet = new HashSet<String>();
			try {
				JMXServiceURL jmxUrl = new JMXServiceURL(String.format(JMX_URL_TEMPLATE,KafkaMongodbMetricsSinkConnector.getConnectJMXPort()));
				JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxUrl, null);
				jmxConnector.connect();
				LOGGER.info("Connected to JMX Server Successfully");
				MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();
				String objName = String.format(SINK_TASK_METRICS_MBEAN_OBJECT_NAME_TEMPLATE, KafkaMongodbMetricsSinkConnector.getConnectorName());
				LOGGER.info("Sink Task Metric Object Name :: {}",objName);
				MBeanAttributeInfo[] attributeInfoArr = mbsc.getMBeanInfo(new ObjectName(objName)).getAttributes();
				LOGGER.info("Retrieved the attrihuteInfo");
				for(MBeanAttributeInfo attributeInfo : attributeInfoArr) {
					allMetricsSet.add(attributeInfo.getName());
				}
				LOGGER.info("Attribute Names: {}",allMetricsSet);
				jmxConnector.close();
				
			} catch (IOException  | MalformedObjectNameException | InstanceNotFoundException | ReflectionException | NumberFormatException | IntrospectionException e) {
				LOGGER.error("An error occurred while retrieving  SinkTaskMetrics", e);
			}
			metricsSet.addAll(allMetricsSet);
			metricsSet.removeAll(getMetricsAsList(KafkaMongodbMetricsSinkConnector.getExcludeMetricsStr()));
		}
		LOGGER.info("Populated MetricsSet:: {}",metricsSet);
		
	}
	

	private List<String> getMetricsAsList(String metricsStr) {
		return Arrays.asList(metricsStr.split(","));
	}


	private AttributeList getJMXMetrics() {
		LOGGER.info("BEGIN::JMXMetricsExporter::getJMXMetrics");
		AttributeList jmxMetricList = null;
		try {
			JMXServiceURL jmxUrl = new JMXServiceURL(String.format(JMX_URL_TEMPLATE,KafkaMongodbMetricsSinkConnector.getConnectJMXPort()));
			JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxUrl, null);
			jmxConnector.connect();
			LOGGER.info("Connected to JMX Server Successfully");
			MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();
			String objName = String.format(SINK_TASK_METRICS_MBEAN_OBJECT_NAME_TEMPLATE, KafkaMongodbMetricsSinkConnector.getConnectorName());
			LOGGER.info("Sink Task Metric Object Name :: {}",objName);
			ObjectName sinkTaskMetricsObjectName = new ObjectName(objName);
			String[] metricsArr = new String[metricsSet.size()];
			jmxMetricList = mbsc.getAttributes(sinkTaskMetricsObjectName, (metricsSet.toArray(metricsArr)));
			LOGGER.info("Retrieved metrics :: {}",jmxMetricList.asList());
			jmxConnector.close();
			
		} catch (IOException  | MalformedObjectNameException | InstanceNotFoundException | ReflectionException | NumberFormatException e) {
			LOGGER.error("An error occurred while retrieving  SinkTaskMetrics", e);
		}
		LOGGER.info("END::JMXMetricsExporter::getJMXMetrics");
		return jmxMetricList;
	}


	private void putMetData(CloudWatchClient cw, Dimension dimension, String metricName, Object metricValue) {
		LOGGER.info("BEGIN::JMXMetricsExporter::putMetData");
		Double dblMetricValue = metricValue != null?Double.valueOf(String.valueOf(metricValue)):0D;
		 try {
	            
		       // Set an Instant object.
		       String time = ZonedDateTime.now(ZoneOffset.UTC).format( DateTimeFormatter.ISO_INSTANT );
		       Instant instant = Instant.parse(time);
	           MetricDatum datum = MetricDatum.builder()
	                .metricName(metricName)
	                .unit(StandardUnit.NONE)
	                .value(dblMetricValue)
	                .timestamp(instant)
	                .dimensions(dimension).build();

	            PutMetricDataRequest request = PutMetricDataRequest.builder()
	                .namespace(KafkaMongodbMetricsSinkConnector.getCWNameSpace())
	                .metricData(datum).build();
	            
	            LOGGER.info("Pushing the metric {}={} to CloudWatch", metricName, metricValue);
	            cw.putMetricData(request);
	            LOGGER.info("Successfully pushed the metric {}={} to CloudWatch", metricName, metricValue);

	        } catch (CloudWatchException e) {
	            LOGGER.error("An error occured while pushing the metrics to CW",e);
	        }
		 LOGGER.info("END::JMXMetricsExporter::putMetData");
		
	}

}
