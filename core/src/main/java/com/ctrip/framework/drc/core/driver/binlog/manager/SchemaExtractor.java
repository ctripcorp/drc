package com.ctrip.framework.drc.core.driver.binlog.manager;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.manager.TableInfo;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class SchemaExtractor {

    public static TableInfo extractColumns(ResultSet resultSet) throws SQLException {

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
            TableMapLogEvent.Column column = new TableMapLogEvent.Column(
                    resultSet.getString(4), nullable,
                    resultSet.getString(8), resultSet.getString(10),
                    resultSet.getString(11), resultSet.getString(12),
                    resultSet.getString(13), resultSet.getString(14),
                    resultSet.getString(15), resultSet.getString(16),
                    resultSet.getString(17), resultSet.getString(18),
                    resultSet.getString(6)
            );

            tableInfo.addColumn(column);
        }

        return tableInfo;
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
