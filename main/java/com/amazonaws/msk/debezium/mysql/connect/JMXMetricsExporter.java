package com.amazonaws.msk.debezium.mysql.connect;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimerTask;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetricsMXBean;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import static com.amazonaws.msk.debezium.mysql.connect.Configuration.*;

public class JMXMetricsExporter extends TimerTask{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(JMXMetricsExporter.class);
	
	
	

	@Override
	public void run() {
		long msBehindSource = getMSBehindSourceMetric();
		pushCWMetric(msBehindSource);
	}


	private void pushCWMetric(long msBehindSource) {
		Region region = Region.of(DebeziumMySqlMetricsConnector.getCWRegion());
        CloudWatchClient cw = CloudWatchClient.builder()
            .region(region)
            .build();
        putMetData(cw, msBehindSource) ;
        cw.close();
	}


	private void putMetData(CloudWatchClient cw, long msBehindSource) {
		 try {
	            Dimension dimension = Dimension.builder()
	                .name("DBServerName")
	                .value(DebeziumMySqlMetricsConnector.getDatabaseServerName())
	                .build();

	            // Set an Instant object.
	            String time = ZonedDateTime.now(ZoneOffset.UTC).format( DateTimeFormatter.ISO_INSTANT );
	            Instant instant = Instant.parse(time);

	            MetricDatum datum = MetricDatum.builder()
	                .metricName("MilliSecondsBehindSource")
	                .unit(StandardUnit.NONE)
	                .value(Double.valueOf(msBehindSource))
	                .timestamp(instant)
	                .dimensions(dimension).build();

	            PutMetricDataRequest request = PutMetricDataRequest.builder()
	                .namespace(DebeziumMySqlMetricsConnector.getCWNameSpace())
	                .metricData(datum).build();

	            cw.putMetricData(request);
	            LOGGER.info("Successfully pushed MilliSecondsBehindSource {} to CloudWatch", msBehindSource);

	        } catch (CloudWatchException e) {
	            LOGGER.error("An error occured while pushing the metrics to CW",e);
	        }
		
	}


	private long getMSBehindSourceMetric() {
		long msBehindSource = 0;
		try {
			//JMXServiceURL jmxUrl = new JMXServiceURL("service:jmx:rmi://localhost/jndi/rmi://localhost:"+connectJMXPort+"/jmxrmi");
			JMXServiceURL jmxUrl = new JMXServiceURL(String.format(JMX_URL_TEMPLATE,DebeziumMySqlMetricsConnector.getConnectJMXPort()));
			LOGGER.info("JMX URL : {}", jmxUrl);
			JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxUrl, null);
			jmxConnector.connect();
			LOGGER.info("Connected to JMX Server Successfully");
			MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();
			//String objName = "debezium.mysql:type=connector-metrics,context=streaming,server="+dbServerName;
			String objName = String.format(STREAMING_MBEAN_OBJECT_NAME_TEMPLATE, DebeziumMySqlMetricsConnector.getDatabaseServerName());
			LOGGER.info("Object Name : {}", objName);
			ObjectName streamingMetricsObjectName = new ObjectName(objName);
			MySqlStreamingChangeEventSourceMetricsMXBean mxbeanProxy = JMX.newMXBeanProxy(mbsc, streamingMetricsObjectName, MySqlStreamingChangeEventSourceMetricsMXBean.class);
			msBehindSource = mxbeanProxy.getMilliSecondsBehindSource();
			LOGGER.info("MilliSecondsBehindSource {}", msBehindSource);
			jmxConnector.close();
			
		} catch (IOException  | MalformedObjectNameException e) {
			LOGGER.error("An error occurred while retrieving  MySqlStreamingChangeEventSourceMetrics", e);
		}
		
		return msBehindSource;
	}

}
