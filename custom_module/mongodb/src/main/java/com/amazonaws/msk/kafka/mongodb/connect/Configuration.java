package com.amazonaws.msk.kafka.mongodb.connect;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public interface Configuration {
	
	String CONNECT_JMX_PORT_KEY = "connect.jmx.port";
	
	String DATABASE_KEY = "database";
	
	String CW_NAMESPACE_KEY = "cloudwatch.namespace.name";
	
	String CW_REGION_KEY = "cloudwatch.region";
	
	String CONNECTOR_NAME_KEY = "name";
	
	String DEFAULT_CW_NAMESPACE = "MSK_Connect";
	
	String DEFAULT_CW_REGION = "us-east-1";
	
	String CW_METRICS_INCLUDE = "cloudwatch.metrics.include";
	
	String CW_METRICS_EXCLUDE = "cloudwatch.metrics.exclude";
	
	Set<String> CW_DEFAULT_SINK_TASK_METRICS = new HashSet<String>(Arrays.asList(new String[]{"records","records-successful",
			"records-failed","latest-kafka-time-difference-ms"}));
	
	int DEFAULT_JMX_PORT = 1098;
	
	long SCHEDULER_INITIAL_DELAY = 60000;
	
	long SCHEDULER_PERIOD = 15000;
	
	String JMX_URL_TEMPLATE = "service:jmx:rmi://localhost/jndi/rmi://localhost:%d/jmxrmi";
	
	String SINK_TASK_METRICS_MBEAN_OBJECT_NAME_TEMPLATE = "com.mongodb.kafka.connect:type=sink-task-metrics,connector=%s,task=sink-task-0";
	
	String LATEST_KAFKA_TIME_DIFFERENCE_MS_METRIC = "latest-kafka-time-difference-ms";
	
}
