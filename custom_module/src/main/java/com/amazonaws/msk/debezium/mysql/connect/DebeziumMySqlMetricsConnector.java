package com.amazonaws.msk.debezium.mysql.connect;

import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static com.amazonaws.msk.debezium.mysql.connect.Configuration.*;

public class DebeziumMySqlMetricsConnector extends MySqlConnector{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumMySqlMetricsConnector.class);
	
	private  static int connectJMXPort = DEFAULT_JMX_PORT;
	
	private static String cwNameSpace = DEFAULT_CW_NAMESPACE;
	
	private static String cwRegion;

	private static String databaseServerName;
	
	private static Scheduler scheduler;

	private static Set<Pattern> metricList = new HashSet<>();

	private static boolean includeMetricOption;
	
	private static Scheduler getScheduler() {
		if(scheduler == null) {
			scheduler =  new Scheduler();
		}
		return scheduler;
	}
	
	
	public static String getDatabaseServerName() {
		return databaseServerName;
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

	public static Set<Pattern> getMetricList(){
		return metricList;
	}

	public static boolean isIncludeMetricOption(){
		return includeMetricOption;
	}
	private void loadMetricsConfiguration(Map<String, String> props){
		// default to include
		includeMetricOption = true;
		String metricListInput = props.get(CW_METRICS_INCLUDE) != null?  String.valueOf(props.get(CW_METRICS_INCLUDE)) : null;
		if (metricListInput == null ){
			metricListInput = props.get(CW_METRICS_EXCLUDE) != null? String.valueOf(props.get(CW_METRICS_EXCLUDE)) : null;
			includeMetricOption = false;
		}
		metricList = Strings.setOfRegex(metricListInput);
	}
	

	@Override
	public void start(Map<String, String> props) {
		
		connectJMXPort = props.get(CONNECT_JMX_PORT_KEY) != null? Integer.parseInt(props.get(CONNECT_JMX_PORT_KEY)) : DEFAULT_JMX_PORT;
		databaseServerName = props.get(DATABASE_SERVER_NAME_KEY) != null? String.valueOf(props.get(DATABASE_SERVER_NAME_KEY)) : "";	
		cwNameSpace = props.get(CW_NAMESPACE_KEY) != null? String.valueOf(props.get(CW_NAMESPACE_KEY)) : DEFAULT_CW_NAMESPACE;
		cwRegion = props.get(CW_REGION_KEY) != null? String.valueOf(props.get(CW_REGION_KEY)) : null;
		loadMetricsConfiguration(props);

		LOGGER.info("Connect JMX Port -  {}  ::  Database Server Name - {} :: CW_NAMESPACE - {} :: CW_REGION - {}", connectJMXPort, databaseServerName, cwNameSpace, cwRegion);
		try {
			try{
				LocateRegistry.createRegistry(connectJMXPort);
			}catch (ExportException ee){
				LOGGER.error(ee.getMessage(),ee);
			}
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			String jmxServiceURL = String.format(JMX_URL_TEMPLATE, connectJMXPort);
			LOGGER.info("JMX Service URL :: {}", jmxServiceURL);
		    JMXServiceURL url = new JMXServiceURL(jmxServiceURL);
		    JMXConnectorServer svr = JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbs);
		    svr.start();
		    LOGGER.info("Started JMX Server Successfully");
		} catch (IOException e) {
			LOGGER.error("Error occurred while starting the JMX Server", e);
		}
		getScheduler().schedule(new JMXMetricsExporter() , SCHEDULER_INITIAL_DELAY, SCHEDULER_PERIOD);
		super.start(props);
	}
	
	
    
}
