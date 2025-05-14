package com.amazonaws.msk.kafka.mongodb.connect;

import static com.amazonaws.msk.kafka.mongodb.connect.Configuration.CONNECTOR_NAME_KEY;
import static com.amazonaws.msk.kafka.mongodb.connect.Configuration.CONNECT_JMX_PORT_KEY;
import static com.amazonaws.msk.kafka.mongodb.connect.Configuration.CW_METRICS_EXCLUDE;
import static com.amazonaws.msk.kafka.mongodb.connect.Configuration.CW_METRICS_INCLUDE;
import static com.amazonaws.msk.kafka.mongodb.connect.Configuration.CW_NAMESPACE_KEY;
import static com.amazonaws.msk.kafka.mongodb.connect.Configuration.CW_REGION_KEY;
import static com.amazonaws.msk.kafka.mongodb.connect.Configuration.DATABASE_KEY;
import static com.amazonaws.msk.kafka.mongodb.connect.Configuration.DEFAULT_CW_NAMESPACE;
import static com.amazonaws.msk.kafka.mongodb.connect.Configuration.DEFAULT_CW_REGION;
import static com.amazonaws.msk.kafka.mongodb.connect.Configuration.DEFAULT_JMX_PORT;
import static com.amazonaws.msk.kafka.mongodb.connect.Configuration.JMX_URL_TEMPLATE;
import static com.amazonaws.msk.kafka.mongodb.connect.Configuration.SCHEDULER_INITIAL_DELAY;
import static com.amazonaws.msk.kafka.mongodb.connect.Configuration.SCHEDULER_PERIOD;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.rmi.registry.LocateRegistry;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.kafka.connect.MongoSinkConnector;

public class KafkaMongodbMetricsSinkConnector extends MongoSinkConnector{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMongodbMetricsSinkConnector.class);

	private  static int connectJMXPort = DEFAULT_JMX_PORT;
	
	private static String cwNameSpace = DEFAULT_CW_NAMESPACE;
	
	private static String cwRegion = DEFAULT_CW_REGION;
	
	private static String database;
	
	private static Scheduler scheduler;
	
	private static String connectorName;
	
	private static String includeMetricsStr;
	
	private static String excludeMetricsStr;
	
	public static String getConnectorName() {
		return connectorName;
	}


	public static void setConnectorName(String connectorName) {
		KafkaMongodbMetricsSinkConnector.connectorName = connectorName;
	}


	private static Scheduler getScheduler() {
		if(scheduler == null) {
			scheduler =  new Scheduler();
		}
		return scheduler;
	}
	
	
	public static String getDatabase() {
		return database;
	}
	
	public static int getConnectJMXPort() {
		return connectJMXPort;
	}
	
	public static String getCWNameSpace() {
		return cwNameSpace;
	}
	
	public static String getCWRegion() {
		return cwRegion;
	}
	
	
	public static String getIncludeMetricsStr() {
		return includeMetricsStr;
	}


	public static String getExcludeMetricsStr() {
		return excludeMetricsStr;
		
	}


	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("BEGIN::KafkaMongodbMetricsSinkConnector::start");
		connectJMXPort = props.get(CONNECT_JMX_PORT_KEY) != null? Integer.valueOf(props.get(CONNECT_JMX_PORT_KEY)) : DEFAULT_JMX_PORT;
		cwNameSpace = props.get(CW_NAMESPACE_KEY) != null? String.valueOf(props.get(CW_NAMESPACE_KEY)) : DEFAULT_CW_NAMESPACE;
		cwRegion = props.get(CW_REGION_KEY) != null? String.valueOf(props.get(CW_REGION_KEY)) : DEFAULT_CW_REGION;
		database = props.get(DATABASE_KEY);	
		connectorName = props.get(CONNECTOR_NAME_KEY);
		includeMetricsStr = props.get(CW_METRICS_INCLUDE);
		excludeMetricsStr = props.get(CW_METRICS_EXCLUDE);
		LOGGER.info("Connector Name :: {}\tIncludeMetricsStr :: {}\tExcludeMetricsStr :: {}", connectorName,includeMetricsStr,excludeMetricsStr);
		try {
			LocateRegistry.createRegistry(connectJMXPort); 
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			String jmxServiceURL = String.format(JMX_URL_TEMPLATE, connectJMXPort);
		    JMXServiceURL url = new JMXServiceURL(jmxServiceURL);
		    JMXConnectorServer svr = JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbs);
		    svr.start();
		    LOGGER.info("Started JMX Server Successfully");
		} catch (IOException e) {
			LOGGER.error("Error occurred while starting the JMX Server", e);
		}
		getScheduler().schedule(new JMXMetricsExporter() , SCHEDULER_INITIAL_DELAY, SCHEDULER_PERIOD);
		super.start(props);
		LOGGER.info("END::KafkaMongodbMetricsSinkConnector::start");
	}
}
