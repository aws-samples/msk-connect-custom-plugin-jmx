package com.amazonaws.msk.debezium.mysql.connect;

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

import static com.amazonaws.msk.debezium.mysql.connect.Configuration.CONNECT_JMX_PORT_KEY;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.CW_DEBEZIUM_SCHEMA_HISTORY_METRICS_EXCLUDE;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.CW_DEBEZIUM_SCHEMA_HISTORY_METRICS_INCLUDE;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.CW_DEBEZIUM_SNAPSHOT_METRICS_EXCLUDE;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.CW_DEBEZIUM_SNAPSHOT_METRICS_INCLUDE;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.CW_DEBEZIUM_STREAM_METRICS_EXCLUDE;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.CW_DEBEZIUM_STREAM_METRICS_INCLUDE;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.CW_NAMESPACE_KEY;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.CW_REGION_KEY;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.DATABASE_SERVER_NAME_KEY;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.DEFAULT_CW_NAMESPACE;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.DEFAULT_JMX_PORT;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.JMX_URL_TEMPLATE;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.SCHEDULER_INITIAL_DELAY;
import static com.amazonaws.msk.debezium.mysql.connect.Configuration.SCHEDULER_PERIOD;

import io.debezium.connector.mysql.MySqlConnector;

public class DebeziumMySqlMetricsConnector extends MySqlConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumMySqlMetricsConnector.class);

    private static int connectJMXPort = DEFAULT_JMX_PORT;

    private static String cwNameSpace = DEFAULT_CW_NAMESPACE;

    private static String cwRegion;

    private static String databaseServerName;

    private static Scheduler scheduler;

    private static String includeStreamingMetricsStr;
    private static String excludeStreamingMetricsStr;

    private static String includeSnapshotMetricsStr;
    private static String excludeSnapshotMetricsStr;

    private static String includeSchemaHistoryMetricsStr;
    private static String excludeSchemaHistoryMetricsStr;

    private static Scheduler getScheduler() {
        if (scheduler == null) {
            scheduler = new Scheduler();
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

    public static String getStreamingIncludeMetricsStr() {
        return includeStreamingMetricsStr;
    }

    public static String getStreamingExcludeMetricsStr() {
        return excludeStreamingMetricsStr;

    }

    public static String getSnapshotIncludeMetricsStr() {
        return includeSnapshotMetricsStr;
    }

    public static String getSnapshotExcludeMetricsStr() {
        return excludeSnapshotMetricsStr;
    }

    public static String getSchemaHistoryIncludeMetricsStr() {
        return includeSchemaHistoryMetricsStr;
    }

    public static String getSchemaHistoryExcludeMetricsStr() {
        return excludeSchemaHistoryMetricsStr;
    }

    @Override
    public void start(Map<String, String> props) {

        LOGGER.info("BEGIN::DebeziumMySqlMetricsConnector::start");
        initializeProperties(props);
        setupJMXServer();
        getScheduler().schedule(new JMXMetricsExporter(), SCHEDULER_INITIAL_DELAY, SCHEDULER_PERIOD);
        super.start(props);
        LOGGER.info("END::DebeziumMySqlMetricsConnector");
    }

    private void initializeProperties(Map<String, String> props) {
        connectJMXPort = Integer.parseInt(props.getOrDefault(CONNECT_JMX_PORT_KEY, String.valueOf(DEFAULT_JMX_PORT)));
        databaseServerName = props.getOrDefault(DATABASE_SERVER_NAME_KEY, "");
        cwNameSpace = props.getOrDefault(CW_NAMESPACE_KEY, DEFAULT_CW_NAMESPACE);
        cwRegion = props.getOrDefault(CW_REGION_KEY, null);
        LOGGER.info("Connect JMX Port - {} :: Database Server Name - {} :: CW_NAMESPACE - {} :: CW_REGION - {}",
                connectJMXPort, databaseServerName, cwNameSpace, cwRegion);

        // loadMetricsConfiguration(props);
        includeStreamingMetricsStr = props.getOrDefault(CW_DEBEZIUM_STREAM_METRICS_INCLUDE, null);
        excludeStreamingMetricsStr = props.getOrDefault(CW_DEBEZIUM_STREAM_METRICS_EXCLUDE, null);
        includeSnapshotMetricsStr = props.getOrDefault(CW_DEBEZIUM_SNAPSHOT_METRICS_INCLUDE, null);
        excludeSnapshotMetricsStr = props.getOrDefault(CW_DEBEZIUM_SNAPSHOT_METRICS_EXCLUDE, null);
        includeSchemaHistoryMetricsStr = props.getOrDefault(CW_DEBEZIUM_SCHEMA_HISTORY_METRICS_INCLUDE, null);
        excludeSchemaHistoryMetricsStr = props.getOrDefault(CW_DEBEZIUM_SCHEMA_HISTORY_METRICS_EXCLUDE, null);
        LOGGER.info("Streaming configuration properties - Include Metrics - {} :: Exclude Metrics - {}",
                includeStreamingMetricsStr, excludeStreamingMetricsStr);
        LOGGER.info("Snapshot configuration properties Include Metrics - {} :: Exclude Metrics - {}",
                includeSnapshotMetricsStr, excludeSnapshotMetricsStr);
        LOGGER.info("Schema History configuration properties - Include Metrics - {} :: Exclude Metrics - {}",
                includeSchemaHistoryMetricsStr, excludeSchemaHistoryMetricsStr);
    }

    private void setupJMXServer() {
        try {
            LocateRegistry.createRegistry(connectJMXPort);
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
    }

}
