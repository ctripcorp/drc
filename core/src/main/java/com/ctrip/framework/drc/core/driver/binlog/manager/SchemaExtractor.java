package com.ctrip.framework.drc.core.driver.binlog.manager;

import com.ctrip.framework.drc.core.driver.binlog.constant.MysqlFieldType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.google.common.collect.Lists;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class SchemaExtractor {

    public static TableInfo extractColumns(ResultSet resultSet) throws SQLException {
        return extractColumns(resultSet, false);
    }

    public static TableInfo extractColumns(ResultSet resultSet, boolean mysql8) throws SQLException {
        TableInfo tableInfo = new TableInfo();

        while (resultSet.next()) {

            tableInfo.setDbName(resultSet.getString(2));
            tableInfo.setTableName(resultSet.getString(3));

            boolean nullable;
            try {
                nullable = resultSet.getBoolean(7);
            } catch (Exception e){
                if ("NO".equalsIgnoreCase(resultSet.getString(7))) {
                    nullable = false;
                } else {
                    nullable = true;
                }
            }
            String columnDefault = resultSet.getString(6);
            String dataTypeLiteral = resultSet.getString(8);

            columnDefault = convertColumnDefault(mysql8, columnDefault, dataTypeLiteral);
            TableMapLogEvent.Column column = new TableMapLogEvent.Column(
                    resultSet.getString(4), nullable,
                    dataTypeLiteral, resultSet.getString(10),
                    resultSet.getString(11), resultSet.getString(12),
                    resultSet.getString(13), resultSet.getString(14),
                    resultSet.getString(15), resultSet.getString(16),
                    resultSet.getString(17), resultSet.getString(18),
                    columnDefault
            );

            tableInfo.addColumn(column);
        }

        return tableInfo;
    }

    protected static String convertColumnDefault(boolean mysql8, String columnDefault, String dataTypeLiteral) {
        if (mysql8 && MysqlFieldType.isBinaryType(dataTypeLiteral) && StringUtils.length(columnDefault) >= 2) {
            try {
                columnDefault = new String(Hex.decodeHex(columnDefault.substring(2).toCharArray()), StandardCharsets.UTF_8);
            } catch (DecoderException e) {
                throw new RuntimeException(e);
            }
        }
        return columnDefault;
    }

    public static List<String> extractValues(ResultSet resultSet, String filtered) throws SQLException {
        List<String> databases = Lists.newArrayList();

        while (resultSet.next()) {
            String value = resultSet.getString(1);
            if (StringUtils.isBlank(filtered) || !filtered.equalsIgnoreCase(value)) {
                databases.add(value);
            }
        }

        return databases;
    }
}
