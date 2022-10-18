package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.core.mq.EventColumn;
import com.ctrip.framework.drc.core.mq.EventData;
import com.ctrip.framework.drc.applier.mq.Mq;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.mq.EventType;
import com.ctrip.framework.drc.core.mq.IProducer;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/10/12
 */
public class MqTransactionContextResource extends TransactionContextResource {

    private final Logger loggerTT = LoggerFactory.getLogger("TRANSACTION TABLE");

    @InstanceResource
    public Mq mq;

    @Override
    public void doInitialize() throws Exception {
    }

    @Override
    public void insert(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        List<EventData> eventDatas = transfer(beforeRows, beforeBitmap, null, null, columns, EventType.INSERT);
        loggerTT.info("insert event data: {}", eventDatas);
        sendEventDatas(eventDatas);
    }


    @Override
    public void update(List<List<Object>> beforeRows, Bitmap beforeBitmap, List<List<Object>> afterRows, Bitmap afterBitmap, Columns columns) {
        List<EventData> eventDatas = transfer(beforeRows, beforeBitmap, afterRows, afterBitmap, columns, EventType.UPDATE);
        loggerTT.info("update event data: {}", eventDatas);
        sendEventDatas(eventDatas);
    }

    @Override
    public void delete(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        List<EventData> eventDatas = transfer(beforeRows, beforeBitmap, null, null, columns, EventType.DELETE);
        loggerTT.info("delete event data: {}", eventDatas);
        sendEventDatas(eventDatas);
    }

    private void sendEventDatas(List<EventData> eventDatas) {
        List<IProducer> producers = mq.getProducers(tableKey.getDatabaseName() + "." + tableKey.getTableName());
        for (IProducer producer : producers) {
            producer.send(eventDatas);
        }
    }

    private List<EventData> transfer(List<List<Object>> beforeRows, Bitmap beforeBitmap, List<List<Object>> afterRows, Bitmap afterBitmap, Columns columns, EventType eventType) {
        List<EventData> eventDatas = Lists.newArrayList();
        Bitmap bitmapOfIdentifier = columns.getBitmapsOfIdentifier().get(0);

        for (int i = 0; i < beforeRows.size(); i++) {
            List<Object> beforeRow = beforeRows.get(i);
            List<Object> afterRow = null;
            if (afterRows != null) {
                afterRow = afterRows.get(i);
            }

            EventData eventData = new EventData();
            eventData.setSchemaName(tableKey.getDatabaseName());
            eventData.setTableName(tableKey.getTableName());
            eventData.setEventType(eventType);
            List<EventColumn> beforeList = new ArrayList<>();
            List<EventColumn> afterList = new ArrayList<>();
            eventData.setBeforeColumns(beforeList);
            eventData.setAfterColumns(afterList);

            List<String> names = columns.getNames();
            for (int j = 0; j < names.size(); j++) {
                boolean isKey = false;
                if (j < bitmapOfIdentifier.size()) {
                    if (bitmapOfIdentifier.get(i)) {
                        isKey = bitmapOfIdentifier.get(j);
                    }
                }

                boolean beforeIsNull = beforeRow.get(j) == null;
                beforeList.add(new EventColumn(names.get(j), beforeIsNull ? null : beforeRow.get(j).toString(), beforeIsNull, isKey, true));
                if (afterRow != null) {
                    boolean afterIsNull = afterRow.get(j) == null;
                    afterList.add(new EventColumn(names.get(j), afterIsNull ? null : afterRow.get(j).toString(), afterIsNull, isKey, true));
                }
            }
            eventDatas.add(eventData);
        }
        return eventDatas;
    }
}
