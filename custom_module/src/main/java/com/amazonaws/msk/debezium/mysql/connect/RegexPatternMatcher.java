package com.amazonaws.msk.debezium.mysql.connect;


import java.util.regex.Pattern;

public class RegexPatternMatcher {

    public static boolean isMatch(String condition){
        // Flag to track if any pattern matches
        boolean matchFound = true;

        // Iterate through each pattern and check for a match
        for (Pattern pattern : DebeziumMySqlMetricsConnector.getMetricList()) {
            matchFound = false;
            if (pattern.matcher(condition).matches()) {
                matchFound = DebeziumMySqlMetricsConnector.isIncludeMetricOption();
                break; // Exit the loop once a match is found
            }
        }

        return matchFound;
    }
}
