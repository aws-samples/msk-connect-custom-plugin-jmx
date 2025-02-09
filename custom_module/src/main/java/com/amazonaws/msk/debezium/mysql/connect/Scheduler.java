package com.amazonaws.msk.debezium.mysql.connect;


import org.slf4j.*;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Class to schedule the JMX metric export.
 */
public class Scheduler {
    private final Timer t = new Timer();
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumMySqlMetricsConnector.class);

    public TimerTask schedule(final Runnable r, long delay, long period) {
        final TimerTask task = new TimerTask() {
            public void run() {
                r.run();
            }
        };
        try{
            t.scheduleAtFixedRate(task, delay,period);
        }catch (RuntimeException ex){
            LOGGER.error("Timer failed",ex);
        }
        return task;
    }
}