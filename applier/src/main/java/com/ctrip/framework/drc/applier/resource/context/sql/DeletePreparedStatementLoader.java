package com.ctrip.framework.drc.applier.resource.context.sql;

import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.fetcher.resource.context.sql.DeleteBuilder;

import java.sql.PreparedStatement;
import java.util.List;

/**
 * @Author Slight
 * Aug 22, 2020
 */
public interface DeletePreparedStatementLoader extends PreparedStatementLoader {

    default PreparedStatement prepareDelete(
            List<String> columnNames,
            List<Object> identifier, Bitmap bitmapOfIdentifier,
            String comment
    ) throws Throwable {

        assert identifier.size() > 0;
        String prepareDelete =
                new DeleteBuilder(fetchTableKey().toString(), columnNames, bitmapOfIdentifier)
                        .prepareWithComment(comment);
        PreparedStatement statement = getConnection().prepareStatement(prepareDelete);
        int i = 0;
        for (Object o : identifier) {
            setValue(statement, ++i, o);
        }
        return statement;
    }
}
