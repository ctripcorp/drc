package com.ctrip.framework.drc.applier.resource.context.sql;

import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.fetcher.resource.context.sql.InsertBuilder;

import java.sql.PreparedStatement;
import java.util.List;

/**
 * @Author Slight
 * Aug 21, 2020
 */
public interface InsertPreparedStatementLoader extends PreparedStatementLoader {

    default PreparedStatement prepareInsert(
            List<String> columnNames, List<Object> values, Bitmap bitmapOfValues,
            String comment
    ) throws Throwable {

        assert values.size() > 0;
        String prepareInsert =
                new InsertBuilder(fetchTableKey().toString(), columnNames, bitmapOfValues)
                        .prepareSingleWithComment(comment);
        PreparedStatement statement = getConnection().prepareStatement(prepareInsert);
        int i = 0;
        for (Object o : values) {
            setValue(statement, ++i, o);
        }
        return statement;
    }
}
