package org.bigdatacenter.healthcarescenarioprocessor.resolver.matcher.extraction;

import org.bigdatacenter.healthcarescenarioprocessor.resolver.matcher.QueryMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

@Component
public class ExtractionQueryMatcherImpl implements ExtractionQueryMatcher {
    private static final Pattern headerPattern = Pattern.compile("(?<=SELECT\\s)[\\w,]+(?=\\sFROM)");
    private static final Pattern dbAndTableNamePattern = Pattern.compile("(?<=FROM\\s)[\\w.]+");
    private static final Pattern dbNamePattern = Pattern.compile("(?<=FROM\\s)\\w+(?=\\.)");
    private static final Pattern tableNamePattern = Pattern.compile("(?<=FROM\\s\\w{1,128}\\.)\\w+");

    private final QueryMatcher queryMatcher;

    @Autowired
    public ExtractionQueryMatcherImpl(QueryMatcher queryMatcher) {
        this.queryMatcher = queryMatcher;
    }

    @Override
    public String getHeader(String query) {
        return queryMatcher.getHeader(query, headerPattern);
    }

    @Override
    public String getDbAndTableName(String query) {
        return queryMatcher.getDbAndTableName(query, dbAndTableNamePattern);
    }

    @Override
    public String getDbName(String query) {
        return queryMatcher.getDbName(query, dbNamePattern);
    }

    @Override
    public String getTableName(String query) {
        return queryMatcher.getTableName(query, tableNamePattern);
    }
}
