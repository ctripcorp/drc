package com.ctrip.framework.drc.applier.resource.context.sql;

import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.fetcher.resource.context.sql.UpdateBuilder;

import java.sql.PreparedStatement;
import java.util.List;

/**
 * @Author Slight
 * Aug 21, 2020
 */
public interface UpdatePreparedStatementLoader extends PreparedStatementLoader {

    default PreparedStatement prepareUpdate0(
            List<String> columnNames,
            List<Object> values, Bitmap bitmapOfValues,
            List<Object> conditions, Bitmap bitmapOfConditions,
            String comment
    ) throws Throwable {

        assert values.size() > 0;
        String prepareUpdate = new UpdateBuilder(fetchTableKey().toString(),
                columnNames, bitmapOfValues, bitmapOfConditions).prepareWithComment(comment);
        PreparedStatement statement = getConnection().prepareStatement(prepareUpdate);
        int i = 0;
        for (Object o : values) {
            setValue(statement, ++i, o);
        }
        for (Object o : conditions) {
            setValue(statement, ++i, o);
        }
        return statement;
    }

    default PreparedStatement prepareUpdate1(
            List<String> columnNames,
            List<Object> values, Bitmap bitmapOfValues,
            List<Object> identifier, Bitmap bitmapOfIdentifier,
            List<Object> valuesOnUpdate, Bitmap bitmapOfValueOnUpdate,
            String comment

    ) throws Throwable {

        assert values.size() > 0;
        String prepareUpdate = new UpdateBuilder(fetchTableKey().toString(),
                columnNames,
                bitmapOfValues, bitmapOfIdentifier, bitmapOfValueOnUpdate).prepareWithComment(comment);
        PreparedStatement statement = getConnection().prepareStatement(prepareUpdate);
        int i = 0;
        for (Object o : values) {
            setValue(statement, ++i, o);
        }
        for (Object o : identifier) {
            setValue(statement, ++i, o);
        }
        for (Object o : valuesOnUpdate) {
            setValue(statement, ++i, o);
        }
        return statement;
    }
}
