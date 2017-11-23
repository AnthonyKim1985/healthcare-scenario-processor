package org.bigdatacenter.healthcarescenarioprocessor.resolver.matcher.extraction;

public interface ExtractionQueryMatcher {
    String getHeader(String query);

    String getDbAndTableName(String query);

    String getDbName(String query);

    String getTableName(String query);
}
