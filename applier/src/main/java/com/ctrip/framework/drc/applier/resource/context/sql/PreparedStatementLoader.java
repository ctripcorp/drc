package com.ctrip.framework.drc.applier.resource.context.sql;

import com.ctrip.framework.drc.fetcher.resource.context.TableKeyContext;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * @Author Slight
 * Aug 21, 2020
 */
public interface PreparedStatementLoader extends TableKeyContext {

    Connection getConnection();

    default void setValue(PreparedStatement statement, int i, Object value) throws SQLException {
        if (value == null) {
            //It's found that 'where key=null' is no valid,
            // use where key is null instead.
            // This feature is not urgent as cond maps is now simplified.
            statement.setNull(i, Types.NULL);
        }
        //Byte Short Integer Long
        else if (value instanceof Byte) {
            Number number = (Number) value;
            statement.setByte(i, number.byteValue());
        } else if (value instanceof Short) {
            Number number = (Number) value;
            statement.setShort(i, number.shortValue());
        } else if (value instanceof Integer) {
            Number number = (Number) value;
            statement.setInt(i, number.intValue());
        } else if (value instanceof Long) {
            Number number = (Number) value;
            statement.setLong(i, number.longValue());
        }
        //String byte[]
        else if (value instanceof String) {
            statement.setString(i, (String) value);
        } else if (value instanceof byte[]) {
            statement.setBytes(i, (byte[]) value);
        }
        //Float Double BigDecimal
        else if (value instanceof Float) {
            statement.setFloat(i, (Float) value);
        } else if (value instanceof Double) {
            statement.setDouble(i, (Double) value);
        } else if (value instanceof BigDecimal) {
            statement.setBigDecimal(i, (BigDecimal) value);
        }
        //Others
        else {
            throw new SQLException("drc applier: unsupported type value. -- " + value);
        }
    }
}
