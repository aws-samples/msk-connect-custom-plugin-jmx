package com.amazonaws.msk.debezium.mysql.connect;

import java.util.*;

public class DebeziumMetricSet {
    protected static Set<String> getStreamingMetrics(){

        Set<String> metricsSet = new HashSet<String>();
        metricsSet.add("NumberOfLargeTransactions");
        metricsSet.add("MilliSecondsSinceLastEvent");
        metricsSet.add("TotalNumberOfEventsSeen");
        metricsSet.add("BinlogPosition");
        metricsSet.add("MilliSecondsBehindSource");
        metricsSet.add("NumberOfErroneousEvents");
        metricsSet.add("NumberOfEventsFiltered");
        metricsSet.add("TotalNumberOfCreateEventsSeen");
        metricsSet.add("TotalNumberOfDeleteEventsSeen");
        metricsSet.add("TotalNumberOfUpdateEventsSeen");
        metricsSet.add("NumberOfCommittedTransactions");
        metricsSet.add("CurrentQueueSizeInBytes");
        metricsSet.add("MaxQueueSizeInBytes");
        metricsSet.add("QueueRemainingCapacity");
        metricsSet.add("QueueTotalCapacity");
        metricsSet.add("NumberOfDisconnects");
        metricsSet.add("NumberOfNotWellFormedTransactions");
        metricsSet.add("NumberOfRolledBackTransactions");
        metricsSet.add("NumberOfSkippedEvents");
        return metricsSet;
    }

    protected static Set<String> getSnapshotMetrics(){

        Set<String> metricsSet = new HashSet<String>();
        metricsSet.add("MilliSecondsSinceLastEvent");
        metricsSet.add("NumberOfErroneousEvents");
        metricsSet.add("NumberOfEventsFiltered");
        metricsSet.add("TotalNumberOfCreateEventsSeen");
        metricsSet.add("TotalNumberOfDeleteEventsSeen");
        metricsSet.add("RecoveryStartTime");
        metricsSet.add("TotalNumberOfEventsSeen");
        metricsSet.add("CurrentQueueSizeInBytes");
        metricsSet.add("MaxQueueSizeInBytes");
        metricsSet.add("QueueRemainingCapacity");
        metricsSet.add("QueueTotalCapacity");
        metricsSet.add("RemainingTableCount");
        metricsSet.add("SnapshotDurationInSeconds");
        metricsSet.add("SnapshotPausedDurationInSeconds");
        metricsSet.add("TotalTableCount");
        return metricsSet;
    }

    protected static Set<String> getSchemaHistoryMetrics(){

        Set<String> metricsSet = new HashSet<String>();
        metricsSet.add("ChangesApplied");
        metricsSet.add("ChangesRecovered");
        metricsSet.add("MilliSecondsSinceLastAppliedChange");
        metricsSet.add("MilliSecondsSinceLastRecoveredChange");
        metricsSet.add("RecoveryStartTime");
        return metricsSet;
    }
}
