package org.bigdatacenter.healthcarescenarioprocessor.resolver.matcher.creation;

public interface CreationQueryMatcher {
    String getHeader(String query);

    String getDbAndTableName(String query);

    String getDbName(String query);

    String getTableName(String query);

    String getSelectQuery(String query);
}
