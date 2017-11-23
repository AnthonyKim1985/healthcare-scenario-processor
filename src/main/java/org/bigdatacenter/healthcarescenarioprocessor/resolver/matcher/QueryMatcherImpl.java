package org.bigdatacenter.healthcarescenarioprocessor.resolver.matcher;

import org.springframework.stereotype.Component;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class QueryMatcherImpl implements QueryMatcher {
    @Override
    public String getHeader(String query, Pattern pattern) {
        final Matcher matcher = pattern.matcher(query);

        String header = null;
        if (matcher.find())
            header = matcher.group();

        if (header == null)
            throw new NullPointerException("The header is null.");

        return header;
    }

    @Override
    public String getDbAndTableName(String query, Pattern pattern) {
        final Matcher matcher = pattern.matcher(query);

        String dbAndTableName = null;
        if (matcher.find())
            dbAndTableName = matcher.group();

        if (dbAndTableName == null)
            throw new NullPointerException("The dbAndTableName is null.");

        return dbAndTableName;
    }

    @Override
    public String getDbName(String query, Pattern pattern) {
        final Matcher matcher = pattern.matcher(query);

        String dbName = null;
        if (matcher.find())
            dbName = matcher.group();

        if (dbName == null)
            throw new NullPointerException("The dbName is null.");

        return dbName;
    }

    @Override
    public String getTableName(String query, Pattern pattern) {
        final Matcher matcher = pattern.matcher(query);

        String tableName = null;
        if (matcher.find())
            tableName = matcher.group();

        if (tableName == null)
            throw new NullPointerException("The tableName is null.");

        return tableName;
    }

    @Override
    public String getSelectQuery(String query, Pattern pattern) {
        final Matcher matcher = pattern.matcher(query);

        String selectQuery = null;
        if (matcher.find())
            selectQuery = matcher.group();

        if (selectQuery == null)
            throw new NullPointerException("The selectQuery is null.");

        return selectQuery;
    }
}
