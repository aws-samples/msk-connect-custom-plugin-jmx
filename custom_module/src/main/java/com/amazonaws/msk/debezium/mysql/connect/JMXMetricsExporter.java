package com.amazonaws.msk.debezium.mysql.connect;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

import org.apache.commons.text.StringEscapeUtils;
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

/**
 * A TimerTask implementation that exports JMX metrics from Debezium MySQL
 * connector to Amazon CloudWatch. This class handles streaming, snapshot, and
 * schema history metrics.
 */
public class JMXMetricsExporter extends TimerTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(JMXMetricsExporter.class);

    //private static Set<String> metricsSet = null;
    private Set<String> streamingMetricsSet = null;
    private Set<String> snapshotMetricsSet = null;
    private Set<String> schemaHistoryMetricsSet = null;

    /**
     * Executes the metrics collection and publishing task. Initializes metric
     * sets and publishes them to CloudWatch if metrics are available.
     */
    @Override
    public void run() {
        LOGGER.info("JMXMetricsExporter started");
        LOGGER.info("JMXMetricsExporter connectJMXPort : {}", DebeziumMySqlMetricsConnector.getConnectJMXPort());

        if (!initializeMetricsSets()) {
            LOGGER.info("No metrics to be populated...");
            return;
        }

        extractAndPublishJMXMetrics();
    }

    /**
     * Initializes the three types of metrics sets: streaming, snapshot, and
     * schema history.
     *
     * @return boolean Returns true if at least one metrics set is non-empty,
     * false otherwise
     */
    protected boolean initializeMetricsSets() {
        LOGGER.info("Initializing metrics sets...");
        streamingMetricsSet = populateMetricsSet("streaming",
                STREAMING_MBEAN_OBJECT_NAME_TEMPLATE,
                DebeziumMySqlMetricsConnector.getStreamingIncludeMetricsStr(),
                DebeziumMySqlMetricsConnector.getStreamingExcludeMetricsStr(),
                CW_DEBEZIUM_DEFAULT_STREAMING_METRICS);
        LOGGER.info("streamingMetricsSet initialized: {}", streamingMetricsSet);

        snapshotMetricsSet = populateMetricsSet("snapshot",
                SNAPSHOT_MBEAN_OBJECT_NAME_TEMPLATE,
                DebeziumMySqlMetricsConnector.getSnapshotIncludeMetricsStr(),
                DebeziumMySqlMetricsConnector.getSnapshotExcludeMetricsStr(),
                CW_DEBEZIUM_DEFAULT_SNAPSHOT_METRICS);
        LOGGER.info("snapshotMetricsSet initialized: {}", snapshotMetricsSet);

        schemaHistoryMetricsSet = populateMetricsSet("schema_history",
                SCHEMA_HISTORY_MBEAN_OBJECT_NAME_TEMPLATE,
                DebeziumMySqlMetricsConnector.getSchemaHistoryIncludeMetricsStr(),
                DebeziumMySqlMetricsConnector.getSchemaHistoryExcludeMetricsStr(),
                CW_DEBEZIUM_DEFAULT_SCHEMA_HISTORY_METRICS);
        LOGGER.info("schemaHistoryMetricsSet initialized: {}", schemaHistoryMetricsSet);

        return !(streamingMetricsSet.isEmpty()
                && snapshotMetricsSet.isEmpty()
                && schemaHistoryMetricsSet.isEmpty());

    }

    /**
     * Populates a set of metrics based on include/exclude filters and default
     * metrics.
     *
     * @param metricType The type of metrics (streaming, snapshot, or
     * schema_history)
     * @param mbeanTemplate The MBean object name template for the metric type
     * @param includeMetricsStr Comma-separated string of metrics to include
     * @param excludeMetricsStr Comma-separated string of metrics to exclude
     * @param defaultMetrics Set of default metrics to use when no
     * include/exclude specified
     * @return Set<String> Returns a set of metric names to be collected
     */
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

        if (defaultMetricsFlag) {
            LOGGER.info("Setting default {} metrics", metricType);
            metricsSet = new HashSet<>(defaultMetrics);
        } else if (includeMetrics && excludeMetrics) {
            //check the value of includemetrics and check if it is equal to ALL then call getAllAvailableMetrics
            LOGGER.info("Both include and exclude metrics are set for {}", metricType);
            //LOGGER.info("includeMetricsStr is {} & excludeMetricsStr is {}" , includeMetricsStr, excludeMetricsStr);
            if (includeMetricsStr.equalsIgnoreCase("ALL")) {
                LOGGER.info("Include metrics is set to ALL for {}", metricType);
                metricsSet = getAllAvailableMetrics(metricType, mbeanTemplate);
                metricsSet.removeAll(getMetricsAsList(excludeMetricsStr));
            } else {
                LOGGER.info("parsing through the specified metric list for {}", metricType);
                metricsSet.addAll(getMetricsAsList(includeMetricsStr));
                metricsSet.removeAll(getMetricsAsList(excludeMetricsStr));
            }
        } else if (includeMetrics) {
            LOGGER.info("Include metrics is set for {}", metricType);
            //LOGGER.info("includeMetricsStr is {} & excludeMetricsStr is {}" , includeMetricsStr, excludeMetricsStr);
            if (includeMetricsStr.equalsIgnoreCase("ALL")) {
                LOGGER.info("Include metrics is set to ALL for {}", metricType);
                Set<String> allMetricsSet = getAllAvailableMetrics(metricType, mbeanTemplate);
                metricsSet.addAll(allMetricsSet);
            } else {
                metricsSet.addAll(getMetricsAsList(includeMetricsStr));
            }
        } else if (excludeMetrics) {
            LOGGER.info("Exclude metrics is set for {}", metricType);
            Set<String> allMetricsSet = getAllAvailableMetrics(metricType, mbeanTemplate);
            metricsSet.addAll(allMetricsSet);
            LOGGER.info("Removing list of excludeMetricsStr metrics: {}", excludeMetricsStr);
            metricsSet.removeAll(getMetricsAsList(excludeMetricsStr));
        }

        LOGGER.info("Populated {} MetricsSet: {}", metricType, metricsSet);
        return metricsSet;
    }

    private List<String> getMetricsAsList(String metricsStr) {
        return Arrays.asList(metricsStr.split(","));
    }

    /**
     * Retrieves all available metrics for a given metric type from JMX.
     *
     * @param metricType The type of metrics to retrieve
     * @param mbeanTemplate The MBean object name template
     * @return Set<String> Returns a set of all available metric names
     */
    private Set<String> getAllAvailableMetrics(String metricType, String mbeanTemplate) {
        Set<String> allMetricsSet = new HashSet<>();
        JMXServiceURL jmxUrl;
        LOGGER.info("Getting all available {} metrics", metricType);

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
                } catch (MalformedObjectNameException | InstanceNotFoundException | IntrospectionException | ReflectionException e) {
                    LOGGER.error("Error getting MBean attributes", e);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error connecting to JMX server: {}", e.getMessage(), e);
        }
        LOGGER.info("All available {} metrics: {}", metricType, allMetricsSet);
        return allMetricsSet;
    }

    /**
     * Extracts metrics from JMX and publishes them to Amazon CloudWatch.
     * Handles streaming, snapshot, and schema history metrics separately.
     */
    protected void extractAndPublishJMXMetrics() {
        CloudWatchClient cw = null;
        try {
            LOGGER.info("Extracting and publishing JMX metrics");
            Region region = Region.of(DebeziumMySqlMetricsConnector.getCWRegion());
            cw = CloudWatchClient.builder()
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
                // Set an Instant object.
                String time = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
                Instant instant = Instant.parse(time);
                // Extract streaming metrics
                if (!streamingMetricsSet.isEmpty()) {
                    LOGGER.info("Extracting streaming metrics: {}", streamingMetricsSet);
                    List<MetricDatum> metrics = new ArrayList<>();
                    Map<String, Map<String, Object>> streamingMetricsAttributes = extractMetricsByType(
                            mbsc,
                            "streaming",
                            STREAMING_MBEAN_OBJECT_NAME_TEMPLATE,
                            streamingMetricsSet
                    );
                    Dimension dimensionType = Dimension.builder()
                            .name("type")
                            .value("streaming")
                            .build();
                    // Process the metrics
                    for (Map.Entry<String, Map<String, Object>> entry : streamingMetricsAttributes.entrySet()) {
                        String metricName = entry.getKey();
                        Map<String, Object> metricData = entry.getValue();
                        String metricType = (String) metricData.get("type");
                        Object metricValue = metricData.get("value");

                        Double dblMetricValue = convertToDouble(metricValue, metricType);

                        MetricDatum datum = MetricDatum.builder()
                                .metricName(metricName)
                                .unit(metricName.toLowerCase().contains("milli")
                                        ? StandardUnit.MILLISECONDS : StandardUnit.NONE)
                                .value(dblMetricValue)
                                .timestamp(instant)
                                .dimensions(dimension, dimensionType)
                                .build();

                        metrics.add(datum);
                        LOGGER.info("Prepared metric: {} of type {} with value {}",
                                metricName, metricType, dblMetricValue);
                    }

                    if (!metrics.isEmpty()) {
                        PutMetricDataRequest request = PutMetricDataRequest.builder()
                                .namespace(DebeziumMySqlMetricsConnector.getCWNameSpace())
                                .metricData(metrics)
                                .build();

                        cw.putMetricData(request);
                        LOGGER.info("Successfully published streaming metrics to CloudWatch");
                    } else {
                        LOGGER.info("No CloudWatch metrics to push for streaming type");
                    }
                }

                // Extract snapshot metrics
                if (!snapshotMetricsSet.isEmpty()) {
                    LOGGER.info("Extracting snapshot metrics: {}", snapshotMetricsSet);
                    List<MetricDatum> metrics = new ArrayList<>();
                    Map<String, Map<String, Object>> snashotMetricsAttributes = extractMetricsByType(
                            mbsc,
                            "snapshot",
                            SNAPSHOT_MBEAN_OBJECT_NAME_TEMPLATE,
                            snapshotMetricsSet
                    );

                    Dimension dimensionType = Dimension.builder()
                            .name("type")
                            .value("snapshot")
                            .build();

                    // Process the metrics
                    for (Map.Entry<String, Map<String, Object>> entry : snashotMetricsAttributes.entrySet()) {
                        String metricName = entry.getKey();
                        Map<String, Object> metricData = entry.getValue();
                        String metricType = (String) metricData.get("type");
                        Object metricValue = metricData.get("value");

                        Double dblMetricValue = convertToDouble(metricValue, metricType);

                        MetricDatum datum = MetricDatum.builder()
                                .metricName(metricName)
                                // amazonq-ignore-next-line
                                .unit(metricName.toLowerCase().contains("milli")
                                        ? StandardUnit.MILLISECONDS : StandardUnit.NONE)
                                .value(dblMetricValue)
                                .timestamp(instant)
                                .dimensions(dimension, dimensionType)
                                .build();

                        metrics.add(datum);
                        LOGGER.info("Prepared metric: {} of type {} with value {}",
                                metricName, metricType, dblMetricValue);
                    }

                    if (!metrics.isEmpty()) {
                        PutMetricDataRequest request = PutMetricDataRequest.builder()
                                .namespace(DebeziumMySqlMetricsConnector.getCWNameSpace())
                                .metricData(metrics)
                                .build();

                        cw.putMetricData(request);
                        LOGGER.info("Successfully published snapshot metrics to CloudWatch");
                    } else {
                        LOGGER.info("No CloudWatch metrics to push for snapshot type");
                    }
                }

                // Extract schema history metrics
                if (!schemaHistoryMetricsSet.isEmpty()) {
                    LOGGER.info("Extracting schema history metrics: {}", schemaHistoryMetricsSet);
                    List<MetricDatum> metrics = new ArrayList<>();

                    Map<String, Map<String, Object>> schemaHistoryMetricsAttributes = extractMetricsByType(
                            mbsc,
                            "schema_history",
                            SCHEMA_HISTORY_MBEAN_OBJECT_NAME_TEMPLATE,
                            schemaHistoryMetricsSet
                    );

                    Dimension dimensionType = Dimension.builder()
                            .name("type")
                            .value("schema_history")
                            .build();

                    // Process the metrics
                    for (Map.Entry<String, Map<String, Object>> entry : schemaHistoryMetricsAttributes.entrySet()) {
                        String metricName = entry.getKey();
                        Map<String, Object> metricData = entry.getValue();
                        String metricType = (String) metricData.get("type");
                        Object metricValue = metricData.get("value");

                        Double dblMetricValue = convertToDouble(metricValue, metricType);

                        MetricDatum datum = MetricDatum.builder()
                                .metricName(metricName)
                                .unit(metricName.toLowerCase().contains("milli")
                                        ? StandardUnit.MILLISECONDS : StandardUnit.NONE)
                                .value(dblMetricValue)
                                .timestamp(instant)
                                .dimensions(dimension, dimensionType)
                                .build();

                        metrics.add(datum);
                        LOGGER.info("Prepared metric: {} of type {} with value {}",
                                metricName, metricType, dblMetricValue);
                    }

                    if (!metrics.isEmpty()) {
                        PutMetricDataRequest request = PutMetricDataRequest.builder()
                                .namespace(DebeziumMySqlMetricsConnector.getCWNameSpace())
                                .metricData(metrics)
                                .build();

                        cw.putMetricData(request);
                        LOGGER.info("Successfully published schema history metrics to CloudWatch");
                    } else {
                        LOGGER.info("No CloudWatch metrics to push for schema history type");
                    }
                }
            } catch (IOException ioEx) {
                LOGGER.error("I/O error during JMX connection or metric extraction", ioEx);
                // Optionally, notify user or system about the failure
            } catch (Exception jmxEx) {
                LOGGER.error("Unexpected error during JMX metric extraction", jmxEx);
                // Optionally, notify user or system about the failure
            }
        } catch (IllegalArgumentException iae) {
            LOGGER.error("Invalid configuration for CloudWatch region or parameters", iae);
            // Optionally, notify user or system about the failure
        } catch (Exception e) {
            LOGGER.error("Error extracting JMX metrics and publishing to CloudWatch", e);
            // Optionally, notify user or system about the failure
        } finally {
            if (cw != null) {
                try {
                    cw.close();
                } catch (Exception closeEx) {
                    LOGGER.warn("Failed to close CloudWatch client", closeEx);
                }
            }
        }
    }

    /**
     * Extracts metrics of a specific type from the JMX MBean server.
     *
     * @param mbsc The MBean server connection
     * @param metricType The type of metrics to extract
     * @param mbeanTemplate The MBean object name template
     * @param metricsSet Set of metric names to extract
     * @return AttributeList Returns list of extracted metric attributes
     */
    private Map<String, Map<String, Object>> extractMetricsByType(MBeanServerConnection mbsc,
            String metricType,
            String mbeanTemplate,
            Set<String> metricsSet) {

        LOGGER.info("Inside extractMetricsByType.Extracting {} metrics", metricType);

        // Using a nested Map: attribute name -> (type, value)
        Map<String, Map<String, Object>> attributesMetadata = new HashMap<>();

        try {
            String objName = String.format(mbeanTemplate,
                    DebeziumMySqlMetricsConnector.getDatabaseServerName());
            ObjectName mbean = new ObjectName(objName);

            // Get MBeanInfo for type information
            Map<String, String> attributeTypes = new HashMap<>();

            // Store attribute types in a map for quick lookup
            for (MBeanAttributeInfo info : mbsc.getMBeanInfo(mbean).getAttributes()) {
                attributeTypes.put(info.getName(), info.getType());
            }

            // Get attribute values
            AttributeList attributes = mbsc.getAttributes(mbean, metricsSet.toArray(new String[0]));

            // Process each attribute
            for (Attribute attr : attributes.asList()) {
                String attrName = attr.getName();
                Object attrValue = attr.getValue();
                String attrType = attributeTypes.get(attrName);

                Map<String, Object> metadata = new HashMap<>();
                metadata.put("type", StringEscapeUtils.escapeHtml4(attrType)); // import org.apache.commons.text.StringEscapeUtils
                metadata.put("value", attrValue);

                attributesMetadata.put(attrName, metadata);

                LOGGER.debug("Processed attribute - Name: {}, Type: {}, Value: {}",
                        attrName, attrType, attrValue);
            }

        } catch (Exception e) {
            LOGGER.error("Error extracting {} metrics", metricType, e);
        }
        return attributesMetadata;
    }

    private Double convertToDouble(Object value, String type) {
        if (value == null) {
            return 0.0;
        }
        try {
            switch (type) {
                case "java.lang.Long":
                case "java.lang.Integer":
                case "java.lang.Double":
                case "java.lang.Float":
                    return ((Number) value).doubleValue();
                case "java.lang.Boolean":
                    return ((Boolean) value) ? 1.0 : 0.0;
                case "[Ljava.lang.String;":
                    String[] arrayValue = (String[]) value;
                    return (double) Arrays.stream(arrayValue)
                            .filter(s -> s != null && !s.trim().isEmpty())
                            .count();
                case "java.util.Map":
                    @SuppressWarnings("unchecked") Map<String, ?> mapValue = (Map<String, ?>) value;
                    return (double) mapValue.size();
                default:
                    return Double.parseDouble(value.toString());
            }
        } catch (Exception e) {
            LOGGER.error("Error converting value {} of type {}", value, type, e);
            return 0.0;
        }
    }

}
