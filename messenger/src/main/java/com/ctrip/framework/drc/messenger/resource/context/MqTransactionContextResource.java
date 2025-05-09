package com.ctrip.framework.drc.messenger.resource.context;

import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.mq.*;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.fetcher.resource.context.TransactionContextResource;
import com.ctrip.framework.drc.fetcher.resource.context.sql.SQLUtil;
import com.ctrip.framework.drc.fetcher.system.InstanceActivity;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.ctrip.framework.drc.messenger.activity.monitor.MqMetricsActivity;
import com.ctrip.framework.drc.messenger.activity.monitor.MqMonitorContext;
import com.ctrip.framework.drc.messenger.mq.MqProvider;
import com.ctrip.framework.drc.messenger.resource.thread.MqRowEventExecutor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.messenger.activity.monitor.MqMetricsActivity.measurementDelay;

/**
 * Created by jixinwang on 2022/10/12
 */
public class MqTransactionContextResource extends TransactionContextResource implements SQLUtil {

    private static final Logger loggerMsgSend = LoggerFactory.getLogger("MESSENGER SEND");

    private static final Charset DEFAULT_BINARY_CHARSET = StandardCharsets.ISO_8859_1;

    @InstanceResource
    public MqProvider mqProvider;

    @InstanceActivity
    public MqMetricsActivity mqMetricsActivity;

    @InstanceConfig(path = "applyMode")
    public int applyMode;

    private AtomicInteger rowsSize;

    private String mqType;
    private ApplyMode mode;

    private static final Map<String, AtomicInteger> activeThreadsMap = Maps.newConcurrentMap();

    //todo: might change to another logEventHeader when multi-thread sending different rowsEvent
    private LogEventHeader logEventHeader;

    public static int getConcurrency(String registryKey) {
        return activeThreadsMap.containsKey(registryKey) ? activeThreadsMap.get(registryKey).get() : 0;
    }

    @InstanceConfig(path = "registryKey")
    public String registryKey;

    @InstanceResource
    public MqRowEventExecutor mqRowEventExecutor;


    @VisibleForTesting
    protected InnerOrderedTransaction orderedTransaction;

    AtomicInteger rowCnt;

    @Override
    public void doInitialize() throws Exception {
        rowsSize = new AtomicInteger(0);
        mqType = MqType.parseByApplyMode(ApplyMode.getApplyMode(applyMode)).name();
        mode = ApplyMode.getApplyMode(applyMode);
        orderedTransaction = new InnerOrderedTransaction();
        rowCnt = new AtomicInteger(0);
        beginTrace("t");
    }

    @Override
    public void doDispose() {
        endTrace("T");
        DefaultEventMonitorHolder.getInstance().logBatchEvent("mq.event", "rows", rowsSize.intValue(), 0);
        DefaultEventMonitorHolder.getInstance().logBatchEvent("mq.event", "gtid", 1, 0);
        DefaultEventMonitorHolder.getInstance().logBatchEvent("mq.event", "xid", 1, 0);
        if (mqType.equals("qmq") && (rowCnt.get() != orderedTransaction.allFutures.size())) {
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.different.row.count.future", registryKey);
        }
        if (mqType.equals("qmq") && (rowsSize.get() != rowCnt.get())) {
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.different.row.count.send", registryKey);
        }
    }

    @Override
    public void insert(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        List<EventData> eventDatas = transfer(beforeRows, beforeBitmap, null, columns, EventType.INSERT);
        loggerMsgSend.info("[GTID][{}] insert event data", fetchGtid());
        sendEventDatas(eventDatas, EventType.INSERT);
    }


    @Override
    public void update(List<List<Object>> beforeRows, Bitmap beforeBitmap, List<List<Object>> afterRows, Bitmap afterBitmap, Columns columns) {
        List<EventData> eventDatas = transfer(beforeRows, beforeBitmap, afterRows, columns, EventType.UPDATE);
        loggerMsgSend.info("[GTID][{}] update event data", fetchGtid());
        sendEventDatas(eventDatas, EventType.UPDATE);
    }

    @Override
    public void delete(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        List<EventData> eventDatas = transfer(beforeRows, beforeBitmap, null, columns, EventType.DELETE);
        loggerMsgSend.info("[GTID][{}] delete event data", fetchGtid());
        sendEventDatas(eventDatas, EventType.DELETE);
    }

    @VisibleForTesting
    protected void sendEventDatas(List<EventData> eventDatas, EventType eventType) {
        List<Producer> producers = mqProvider.getProducers(tableKey.getDatabaseName() + "." + tableKey.getTableName());
        for (Producer producer : producers) {
            switch (mode) {
                case kafka:
                    sendAndReport(eventDatas, eventType, producer);
                    break;
                case mq:
                    for (EventData data : eventDatas) {
                        orderedTransaction.onSendAndReport(new RowSendHandler(data, producer));
                    }
                    rowCnt.getAndAdd(eventDatas.size());
                    break;
            }

        }

        if (progress != null) {
            progress.tick();
        }
    }

    @VisibleForTesting
    protected boolean sendAndReport(List<EventData> eventDatas, EventType eventType, Producer producer) {
        AtomicInteger atomicInteger = activeThreadsMap.computeIfAbsent(registryKey, (key) -> new AtomicInteger(0));
        atomicInteger.getAndIncrement();
        try {
            reportHickWall(eventDatas,System.currentTimeMillis() - logEventHeader.getEventTimestamp() * 1000, measurementDelay, mqType);
            boolean send = producer.send(eventDatas, eventType);
            rowsSize.getAndAdd(eventDatas.size());
            reportHickWall(eventDatas, producer.getTopic(), mqType, send);
            return send;
        } finally {
            atomicInteger.getAndDecrement();
        }
    }


    private List<EventData> transfer(List<List<Object>> beforeRows, Bitmap beforeBitmap, List<List<Object>> afterRows, Columns columns, EventType eventType) {
        DcTag dcTag = fetchDcTag();
        List<EventData> eventDatas = Lists.newArrayList();

        Bitmap bitmapOfIdentifier = columns.getBitmapsOfIdentifier().get(0);
        if (bitmapOfIdentifier == null) {
            bitmapOfIdentifier = new Bitmap();
        }

        Columns selectColumns = selectColumns(columns, beforeBitmap);
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

            for (int j = 0; j < selectColumns.size(); j++) {
                boolean isKey = false;
                if (j < bitmapOfIdentifier.size()) {
                    if (bitmapOfIdentifier.get(j)) {
                        isKey = bitmapOfIdentifier.get(j);
                    }
                }

                TableMapLogEvent.Column column = selectColumns.get(j);
                String columnName = column.getName();

                boolean beforeIsNull = beforeRow.get(j) == null;
                String beforeColumnValue = beforeIsNull ? null : parseOneValue(beforeRow.get(j), column);

                switch (eventType) {
                    case UPDATE:
                        beforeList.add(new EventColumn(columnName, beforeColumnValue, beforeIsNull, isKey, false));
                        if (afterRow != null) {
                            boolean afterIsNull = afterRow.get(j) == null;
                            String afterColumnValue = afterIsNull ? null : parseOneValue(afterRow.get(j), column);
                            afterList.add(new EventColumn(columnName, afterColumnValue, afterIsNull, isKey, !StringUtils.equals(beforeColumnValue, afterColumnValue)));
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

    private String parseOneValue(Object value, TableMapLogEvent.Column column) {
        if (column.isBinary()) {
            Charset charset = column.calJavaCharset();
            try {
                if (charset == null) {
                    charset = DEFAULT_BINARY_CHARSET;
                }
                return new String((byte[]) value, charset);
            } catch (UnsupportedCharsetException e) {
                loggerMsgSend.error("parse one value of row error with charset {}: ", charset, e);
            }
        }
        return value.toString();
    }

    private void reportHickWall(List<EventData> eventDatas, String topic, String mqType, boolean send) {
        if (!eventDatas.isEmpty()) {
            EventData eventData = eventDatas.get(0);
            MqMonitorContext mqMonitorContext = new MqMonitorContext(eventData.getSchemaName(), eventData.getTableName(), eventDatas.size(), eventData.getEventType(), eventData.getDcTag(), topic, mqType, send);
            mqMetricsActivity.report(mqMonitorContext);
        }
    }

    private void reportHickWall(List<EventData> eventDatas, long timeCost, String metricName, String mqType) {
        if (!eventDatas.isEmpty()) {
            EventData eventData = eventDatas.get(0);
            MqMonitorContext mqMonitorContext = new MqMonitorContext(eventData.getSchemaName(), timeCost, registryKey,metricName, mqType);
            mqMetricsActivity.report(mqMonitorContext);
        }

    }


    @Override
    public void begin() {
    }

    @Override
    public void rollback() {

    }

    @Override
    public void commit() {

    }

    @Override
    public void setGtid(String gtid) {

    }

    @Override
    public void recordTransactionTable(String gtid) {

    }

    @Override
    public boolean everConflict() {
        return false;
    }

    @Override
    public boolean everRollback() {
        return false;
    }

    @Override
    public Queue<String> getLogs() {
        return null;
    }

    @Override
    public Throwable getLastUnbearable() {
        return null;
    }


    @Override
    public void setLogEventHeader(LogEventHeader logEventHeader) {
        this.logEventHeader = logEventHeader;
    }

    @Override
    public TransactionData.ApplyResult complete() {
        try {
            orderedTransaction.waitSendResults();
        } catch (ExecutionException e) {
            loggerMsgSend.error("[mqRowEventExecutor] ExecutionException, in {}", registryKey,e);
            orderedTransaction.cancelFutures();
            throw new RuntimeException(e);
        }

        return TransactionData.ApplyResult.SUCCESS;
    }



    public static class InnerOrderedTransaction {
        @VisibleForTesting
        protected ConcurrentHashMap<RowSendHandler.RowKey, CompletableFuture<Boolean>> depends = new ConcurrentHashMap<>();
        @VisibleForTesting
        protected List<CompletableFuture<Boolean>> allFutures = Lists.newArrayList();


        public void onSendAndReport(RowSendHandler handler) {
            CompletableFuture<Boolean> dependFuture = depends.getOrDefault(handler.key, null);
            handler.onSendAndReport(dependFuture);
            depends.put(handler.key, handler.sendfuture);
            allFutures.add(handler.sendfuture);
        }


        public void waitSendResults() throws ExecutionException {
            for (CompletableFuture<Boolean> future : allFutures) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    loggerMsgSend.warn("[mqRowEventExecutor] InterruptedException", e);
                    cancelFutures();
                    Thread.currentThread().interrupt();
                }
            }
        }

        public void cancelFutures(){
            for (Future<Boolean> f : allFutures) {
                f.cancel(true);
            }
        }

    }


    public class RowSendHandler {
        protected final EventData row;
        private final Producer producer;
        private CompletableFuture<Boolean> sendfuture;
        protected static final Logger logger = LoggerFactory.getLogger("ORDER TRANSACTION");
        private final RowKey key;

        public RowSendHandler(EventData data, Producer producer) {
            this.row = data;
            this.producer = producer;
            this.key = buildKey();
        }

        public final RowKey buildKey() {
            List<EventColumn> columns = row.getEventType() == EventType.INSERT ? row.getAfterColumns() : row.getBeforeColumns();
            String primaryKeyPattern = columns.stream().filter(EventColumn::isKey).map(EventColumn::getColumnValue).collect(Collectors.joining(";"));
            return new RowKey(row.getSchemaName(), row.getTableName(), primaryKeyPattern, producer.getTopic());
        }


        public void onSendAndReport(CompletableFuture<Boolean> dependFuture) {
            if (dependFuture != null) {
                this.sendfuture = mqRowEventExecutor.thenApplyAsync(dependFuture, result -> sendAndReport(Lists.newArrayList(row), row.getEventType(), producer));
            } else {
                this.sendfuture = mqRowEventExecutor.supplyAsync(() -> sendAndReport(Lists.newArrayList(row), row.getEventType(), producer));
            }
        }

        public static class RowKey {
            String schemaName;
            String tableName;
            String primaryKey;
            String topic;

            public RowKey(String schemaName, String tableName, String primaryKey, String topic) {
                this.schemaName = schemaName;
                this.tableName = tableName;
                this.primaryKey = primaryKey;
                this.topic = topic;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }
                if (obj == null || getClass() != obj.getClass()) {
                    return false;
                }
                RowKey other = (RowKey) obj;
                return schemaName.equals(other.schemaName) &&
                        tableName.equals(other.tableName) &&
                        primaryKey.equals(other.primaryKey) &&
                        topic.equals(other.topic);
            }

            @Override
            public int hashCode() {
                return Objects.hash(schemaName, tableName, primaryKey, topic);
            }
        }

    }
}
