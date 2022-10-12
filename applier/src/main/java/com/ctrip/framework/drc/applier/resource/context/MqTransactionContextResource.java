package com.ctrip.framework.drc.applier.resource.context;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.google.common.collect.Lists;
import muise.ctrip.canal.ColumnData;
import muise.ctrip.canal.DataChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/10/12
 */
public class MqTransactionContextResource extends TransactionContextResource {

    private final Logger loggerTT = LoggerFactory.getLogger("TRANSACTION TABLE");

    @Override
    public void insert(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        List<String> rowsData = transfer(beforeRows, beforeBitmap, null, null, columns, EventType.INSERT);
        loggerTT.info("insert rows date: {}", rowsData);
    }


    @Override
    public void update(List<List<Object>> beforeRows, Bitmap beforeBitmap, List<List<Object>> afterRows, Bitmap afterBitmap, Columns columns) {
        List<String> rowsData = transfer(beforeRows, beforeBitmap, afterRows, afterBitmap, columns, EventType.UPDATE);
        loggerTT.info("update rows date: {}", rowsData);
    }

    @Override
    public void delete(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        List<String> rowsData = transfer(beforeRows, beforeBitmap, null, null, columns, EventType.DELETE);
        loggerTT.info("delete rows date: {}", rowsData);
    }


    private List<String> transfer(List<List<Object>> beforeRows, Bitmap beforeBitmap, List<List<Object>> afterRows, Bitmap afterBitmap, Columns columns, EventType eventType) {
        List<String> rowsData = Lists.newArrayList();
        Bitmap bitmapOfIdentifier = columns.getBitmapsOfIdentifier().get(0);

        for (int i = 0; i < beforeRows.size(); i++) {
            List<Object> beforeRow = beforeRows.get(i);
            List<Object> afterRow = null;
            if (afterRows != null) {
                afterRow = afterRows.get(i);
            }

            DataChange dataChange = new DataChange();
            dataChange.setSchemaName(tableKey.getDatabaseName());
            dataChange.setTableName(tableKey.getTableName());
            dataChange.setEventType(eventType.toString());
            List<ColumnData> beforeList = new ArrayList<>();
            List<ColumnData> afterList = new ArrayList<>();
            dataChange.setBeforeColumnList(beforeList);
            dataChange.setAfterColumnList(afterList);

            List<String> names = columns.getNames();
            for (int j = 0; j < names.size(); j++) {
                boolean isKey = false;
                if (j < bitmapOfIdentifier.size()) {
                    if (bitmapOfIdentifier.get(i)) {
                        isKey = bitmapOfIdentifier.get(j);
                    }
                }

                boolean isNull = beforeRow.get(j) == null;
                beforeList.add(new ColumnData(names.get(j), isNull ? null : beforeRow.get(j).toString(), true, isKey, isNull));
                if (afterRow != null) {
                    afterList.add(new ColumnData(names.get(j), afterRow.get(j).toString(), true, isKey, false));
                }
            }

            JSONObject jsonObject = JSON.parseObject(dataChange.toString());
            rowsData.add(jsonObject.toJSONString());
        }

        return rowsData;
    }
}
