package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;

import java.util.List;

/**
 * @Author limingdong
 * @create 2020/11/1
 */
public interface SchemaContext extends Context.Simple {

    String KEY_NAME = "schema columns";

    default void updateColumns(List<TableMapLogEvent.Column> columns) {
        update(KEY_NAME, columns);
    }

    default Columns fetchColumns() {
        return Columns.from((List<TableMapLogEvent.Column>) fetch(KEY_NAME));
    }
}
