package com.ctrip.framework.drc.messenger.resource.context;

import com.ctrip.framework.drc.core.mq.*;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.messenger.activity.monitor.MqMetricsActivity.measurementDelay;

/**
 * Created by dengquanliang
 * 2025/5/29 17:25
 */
public class QmqTransactionContextResource extends MqTransactionContextResource {

    @VisibleForTesting
    protected InnerOrderedTransaction orderedTransaction;


    @Override
    public void doInitialize() throws Exception {
        super.doInitialize();
        orderedTransaction = new InnerOrderedTransaction();
    }


    @Override
    protected void send(List<EventData> eventDatas, EventType eventType, Producer producer) {
        for (EventData data : eventDatas) {
            orderedTransaction.onSendAndReport(new RowSendHandler(data, producer));
        }
    }

    @VisibleForTesting
    protected boolean sendAndReport(List<EventData> eventDatas, EventType eventType, Producer producer) {
        AtomicInteger atomicInteger = activeThreadsMap.computeIfAbsent(registryKey, (key) -> new AtomicInteger(0));
        atomicInteger.getAndIncrement();
        try {
            reportHickWall(eventDatas,System.currentTimeMillis() - logEventHeader.getEventTimestamp() * 1000, measurementDelay, MqType.qmq.name());
            boolean send = producer.sendQmq(eventDatas, eventType);
            rowsSize.getAndAdd(eventDatas.size());
            reportHickWall(eventDatas, producer.getTopic(), MqType.qmq.name(), send);
            return send;
        } finally {
            atomicInteger.getAndDecrement();
        }
    }

    @Override
    public TransactionData.ApplyResult complete() {
        orderedTransaction.waitSendResults();
        return TransactionData.ApplyResult.SUCCESS;
    }


    public class InnerOrderedTransaction {
        @VisibleForTesting
        protected ConcurrentHashMap<RowSendHandler.RowKey, RowSendHandler> depends = new ConcurrentHashMap<>();
        @VisibleForTesting
        protected ConcurrentHashMap<RowSendHandler, CompletableFuture<Boolean>> handlerFuturesInProcessing  = new ConcurrentHashMap<>();
        protected Throwable ex;

        public void onSendAndReport(RowSendHandler handler) {
            RowSendHandler dependHandler = depends.getOrDefault(handler.key, null);
            if (ex != null) {
                waitSendResults();
            }
            handler.setTransaction(this);
            CompletableFuture<Boolean> dependFuture = dependHandler != null ? dependHandler.sendfuture : null;
            handler.onSendAndReport(dependFuture);
            depends.put(handler.key, handler);
            handlerFuturesInProcessing.put(handler, handler.sendfuture);
        }

        public void onComplete(RowSendHandler handler, Throwable e) {
            depends.remove(handler.key, handler);
            handlerFuturesInProcessing.remove(handler);
            if (e != null) {
                ex = e;
            }
        }


        public void waitSendResults() {
            InterruptedException interruptedException = null;
            ExecutionException executionException = null;
            for (CompletableFuture<Boolean> future : handlerFuturesInProcessing.values()) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    interruptedException = e;
                } catch (ExecutionException e) {
                    executionException = e;
                }
            }
            if (interruptedException != null) {
                loggerMsgSend.error("[mqRowEventExecutor] InterruptedException, server may stopped: {}.", registryKey, interruptedException);
                Thread.currentThread().interrupt();
            }
            if (executionException != null) {
                loggerMsgSend.error("[mqRowEventExecutor] ExecutionException, in {}.", registryKey, executionException);
                throw new RuntimeException(executionException);
            }
            if (ex != null) {
                loggerMsgSend.error("[InnerOrderedTransaction] exception, in {}.", registryKey, ex);
                throw new RuntimeException(ex);
            }
        }

    }


    public class RowSendHandler {
        protected final EventData row;
        private final Producer producer;
        private CompletableFuture<Boolean> sendfuture;
        private final RowKey key;
        private InnerOrderedTransaction transaction;

        public RowSendHandler(EventData data, Producer producer) {
            this.row = data;
            this.producer = producer;
            this.key = buildKey();
        }

        public void setTransaction(InnerOrderedTransaction transaction) {
            this.transaction = transaction;
        }

        public final RowKey buildKey() {
            List<EventColumn> columns = row.getEventType() == EventType.INSERT ? row.getAfterColumns() : row.getBeforeColumns();
            String primaryKeyPattern = columns.stream().filter(EventColumn::isKey).map(EventColumn::getColumnValue).collect(Collectors.joining(";"));
            return new RowKey(row.getSchemaName(), row.getTableName(), primaryKeyPattern, producer.getTopic());
        }


        public void onSendAndReport(CompletableFuture<Boolean> dependFuture) {
            if (dependFuture != null) {
                this.sendfuture = mqRowEventExecutor.thenApplyAsync(
                        dependFuture,
                        result -> sendAndReport(Lists.newArrayList(row), row.getEventType(), producer),
                        this
                );
            } else {
                this.sendfuture = mqRowEventExecutor.supplyAsync(
                        () -> sendAndReport(Lists.newArrayList(row), row.getEventType(), producer),
                        this
                );
            }
        }

        public void onComplete(Throwable e) {
            transaction.onComplete(this, e);
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
