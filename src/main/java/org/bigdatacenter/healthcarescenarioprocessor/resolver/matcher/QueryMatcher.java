package org.bigdatacenter.healthcarescenarioprocessor.resolver.matcher;

import java.util.regex.Pattern;

public interface QueryMatcher {
    String getHeader(String query, Pattern pattern);

    String getDbAndTableName(String query, Pattern pattern);

    String getDbName(String query, Pattern pattern);

    String getTableName(String query, Pattern pattern);

    String getSelectQuery(String query, Pattern pattern);
}
