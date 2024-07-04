package com.amazonaws.msk.debezium.mysql.connect;

public interface Configuration {
	
	String CONNECT_JMX_PORT_KEY = "connect.jmx.port";
	
	String DATABASE_SERVER_NAME_KEY = "database.server.name";
	
	String CW_NAMESPACE_KEY = "cloudwatch.namespace.name";
	
	String CW_REGION_KEY = "cloudwatch.region";
	
	String DEFAULT_CW_NAMESPACE = "MSK_Connect";
	
	int DEFAULT_JMX_PORT = 1098;
	
	long SCHEDULER_INITIAL_DELAY = 60000;
	
	long SCHEDULER_PERIOD = 15000;
	
	String JMX_URL_TEMPLATE = "service:jmx:rmi://localhost/jndi/rmi://localhost:%d/jmxrmi";
	
	String STREAMING_MBEAN_OBJECT_NAME_TEMPLATE = "debezium.mysql:type=connector-metrics,context=streaming,server=%s";
}
