package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.applier.activity.monitor.MqMetricsActivity;
import com.ctrip.framework.drc.applier.activity.monitor.MqMonitorContext;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.mq.*;
import com.ctrip.framework.drc.applier.mq.MqProvider;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.fetcher.resource.context.sql.SQLUtil;
import com.ctrip.framework.drc.fetcher.system.InstanceActivity;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/10/12
 */
public class MqTransactionContextResource extends TransactionContextResource implements SQLUtil {

    private static final Logger loggerMsgSend = LoggerFactory.getLogger("MESSENGER SEND");

    @InstanceResource
    public MqProvider mqProvider;

    @InstanceActivity
    public MqMetricsActivity mqMetricsActivity;

    private int rowsSize;

    @Override
    public void doInitialize() throws Exception {
        rowsSize = 0;
    }

    @Override
    public void doDispose() {
        DefaultEventMonitorHolder.getInstance().logBatchEvent("mq.event", "rows", rowsSize, 0);
        DefaultEventMonitorHolder.getInstance().logBatchEvent("mq.event", "gtid", 1, 0);
        DefaultEventMonitorHolder.getInstance().logBatchEvent("mq.event", "xid", 1, 0);
    }

    @Override
    public void insert(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        List<EventData> eventDatas = transfer(beforeRows, beforeBitmap, null, columns, EventType.INSERT);
        loggerMsgSend.info("[GTID][{}] insert event data", fetchGtid());
        sendEventDatas(eventDatas);
    }


    @Override
    public void update(List<List<Object>> beforeRows, Bitmap beforeBitmap, List<List<Object>> afterRows, Bitmap afterBitmap, Columns columns) {
        List<EventData> eventDatas = transfer(beforeRows, beforeBitmap, afterRows, columns, EventType.UPDATE);
        loggerMsgSend.info("[GTID][{}] update event data", fetchGtid());
        sendEventDatas(eventDatas);
    }

    @Override
    public void delete(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        List<EventData> eventDatas = transfer(beforeRows, beforeBitmap, null, columns, EventType.DELETE);
        loggerMsgSend.info("[GTID][{}] delete event data", fetchGtid());
        sendEventDatas(eventDatas);
    }

    private void sendEventDatas(List<EventData> eventDatas) {
        List<Producer> producers = mqProvider.getProducers(tableKey.getDatabaseName() + "." + tableKey.getTableName());
        for (Producer producer : producers) {
            producer.send(eventDatas);
            rowsSize += eventDatas.size();
            reportHickWall(eventDatas);
        }
    }

    private List<EventData> transfer(List<List<Object>> beforeRows, Bitmap beforeBitmap, List<List<Object>> afterRows, Columns columns, EventType eventType) {
        DcTag dcTag = fetchDcTag();
        List<EventData> eventDatas = Lists.newArrayList();

        Bitmap bitmapOfIdentifier = columns.getBitmapsOfIdentifier().get(0);
        if (bitmapOfIdentifier == null) {
            bitmapOfIdentifier = new Bitmap();
        }

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
            eventData.setDcTag(dcTag);
            List<EventColumn> beforeList = new ArrayList<>();
            List<EventColumn> afterList = new ArrayList<>();
            eventData.setBeforeColumns(beforeList);
            eventData.setAfterColumns(afterList);

            List<String> names = selectColumnNames(columns.getNames(), beforeBitmap);
            for (int j = 0; j < names.size(); j++) {
                boolean isKey = false;
                if (j < bitmapOfIdentifier.size()) {
                    if (bitmapOfIdentifier.get(j)) {
                        isKey = bitmapOfIdentifier.get(j);
                    }
                }

                String columnName = names.get(j);
                boolean beforeIsNull = beforeRow.get(j) == null;
                String beforeColumnValue = beforeIsNull ? null : beforeRow.get(j).toString();

                switch (eventType) {
                    case UPDATE:
                        beforeList.add(new EventColumn(columnName, beforeColumnValue, beforeIsNull, isKey, false));
                        if (afterRow != null) {
                            boolean afterIsNull = afterRow.get(j) == null;
                            String afterColumnValue = afterIsNull ? null : afterRow.get(j).toString();
                            afterList.add(new EventColumn(columnName, afterColumnValue, afterIsNull, isKey, StringUtils.equals(beforeColumnValue, afterColumnValue)));
                        }
                        break;
                    case INSERT:
                        afterList.add(new EventColumn(columnName, beforeColumnValue, beforeIsNull, isKey, true));
                        break;
                    case DELETE:
                        beforeList.add(new EventColumn(columnName, beforeColumnValue, beforeIsNull, isKey, false));
                        break;
                }
            }
            eventDatas.add(eventData);
        }
        return eventDatas;
    }

    private void reportHickWall(List<EventData> eventDatas) {
        if (!eventDatas.isEmpty()) {
            EventData eventData = eventDatas.get(0);
            MqMonitorContext mqMonitorContext = new MqMonitorContext(eventData.getSchemaName(), eventData.getTableName(), eventDatas.size(), eventData.getEventType(), eventData.getDcTag());
            mqMetricsActivity.report(mqMonitorContext);
        }
    }
}
