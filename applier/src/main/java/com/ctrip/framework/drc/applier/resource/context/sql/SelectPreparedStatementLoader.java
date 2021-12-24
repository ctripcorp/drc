package com.ctrip.framework.drc.applier.resource.context.sql;

import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.fetcher.resource.context.sql.SelectBuilder;

import java.sql.PreparedStatement;
import java.util.List;

/**
 * @Author Slight
 * Aug 21, 2020
 */
public interface SelectPreparedStatementLoader extends PreparedStatementLoader {

    default PreparedStatement prepareSelect(
            List<String> columnNames,
            List<Object> identifier, Bitmap bitmapOfIdentifier,
            String comment
    ) throws Throwable {

        assert identifier.size() > 0;
        String prepareSelect =
                new SelectBuilder(fetchTableKey().toString(), columnNames, bitmapOfIdentifier)
                        .prepareWithComment(comment);
        PreparedStatement statement = getConnection().prepareStatement(prepareSelect);
        int i = 0;
        for (Object o : identifier) {
            setValue(statement, ++i, o);
        }
        return statement;
    }
}
