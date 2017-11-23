package org.bigdatacenter.healthcarescenarioprocessor.resolver.matcher.creation;

import org.bigdatacenter.healthcarescenarioprocessor.resolver.matcher.QueryMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

@Component
public class CreationQueryMatcherImpl implements CreationQueryMatcher {
    private static final Logger logger = LoggerFactory.getLogger(CreationQueryMatcherImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private static final Pattern headerPattern = Pattern.compile("(?<=SELECT\\s)[\\w,]+(?=\\sFROM)");
    private static final Pattern dbAndTableNamePattern = Pattern.compile("(?<=CREATE\\sTABLE\\s)\\w+\\.\\w+(?=\\sSTORED)");
    private static final Pattern dbNamePattern = Pattern.compile("(?<=CREATE\\sTABLE\\s)\\w+(?=\\.)");
    private static final Pattern tableNamePattern = Pattern.compile("(?<=CREATE\\sTABLE\\s\\w{1,128}\\.)\\w+");
    private static final Pattern selectQueryPattern = Pattern.compile("(?<=STORED\\sAS\\sORC\\sAS\\s)\\w.+");

    private final QueryMatcher queryMatcher;

    @Autowired
    public CreationQueryMatcherImpl(QueryMatcher queryMatcher) {
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

    @Override
    public String getSelectQuery(String query) {
        return queryMatcher.getSelectQuery(query, selectQueryPattern);
    }
}
