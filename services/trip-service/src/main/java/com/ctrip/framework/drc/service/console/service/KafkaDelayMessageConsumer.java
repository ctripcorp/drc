package com.ctrip.framework.drc.service.console.service;

import com.ctrip.framework.ckafka.client.KafkaClientFactory;
import com.ctrip.framework.ckafka.codec.deserializer.HermesJsonDeserializer;
import com.ctrip.framework.ckafka.codec.entity.HermesConsumerConfig;
import com.ctrip.framework.drc.core.monitor.column.DelayInfo;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.mq.EventType;
import com.ctrip.framework.drc.core.mq.IKafkaDelayMessageConsumer;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.drc.service.config.TripServiceDynamicConfig;
import com.ctrip.framework.drc.service.mq.DataChangeMessage;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by shiruixin
 * 2025/1/9 13:45
 */
public class KafkaDelayMessageConsumer implements IKafkaDelayMessageConsumer {

    private static final Logger logger = LoggerFactory.getLogger("delayMonitorLogger");

    private static final int DC_INDEX = 1;
    private static final int DELAY_INFO_INDEX = 2;
    private static final int DATA_CHANGE_TIME_INDEX = 3;
    private static final long TOLERANCE_TIME = 3 * 60000L;
    private static final long HUGE_VAL = 60 * 60000L;
    private static final String MQ_DELAY_MEASUREMENT = "fx.drc.messenger.delay";

    private Consumer<String,String> kafkaConsumer;
    private final ExecutorService kafkaConsumeService= ThreadUtils.newSingleThreadExecutor("DRC-Kafka-Consume");
    private final ExecutorService executorService = ThreadUtils.newFixedThreadPool(5, "DRC-Kafka-Consume-Process");
    private Future future;

    private Set<String> dcsRelated = Sets.newHashSet();
    private volatile Set<String> mhasRelated = Sets.newConcurrentHashSet();
    private volatile Map<String, String> mha2Dc = Maps.newConcurrentMap();

    // k: mhaInfo ,v :receiveTime
    private final Map<MhaInfo,Long> receiveTimeMap = Maps.newConcurrentMap();
    private final ScheduledExecutorService checkScheduledExecutor =
            ThreadUtils.newSingleThreadScheduledExecutor("MessengerDelayMonitor");
    private ScheduledFuture<?> checkTaskFuture;

    //in case duplicate consumption
    private final Map<String, Pair<Integer, Long>> mhaLastReceiveMap = Maps.newConcurrentMap();

    private Properties kafkaConf;
    private String subject;
    private String consumerGroup;

    @Override
    public void initConsumer(String subject, String consumerGroup, Set<String> dcs) {
        try {
            this.dcsRelated = dcs;
            this.subject = subject;
            this.consumerGroup = consumerGroup;
            kafkaConf = new Properties();
            kafkaConf.put(ConsumerConfig.CLIENT_ID_CONFIG, TripServiceDynamicConfig.getInstance().getKafkaAppidToken() + "-drcconsole");
            kafkaConf.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
            kafkaConf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
            kafkaConf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HermesJsonDeserializer.class.getCanonicalName());
            kafkaConf.put(HermesConsumerConfig.HERMES_MESSAGE_CLASS_CONFIG, String.class.getCanonicalName());
            kafkaConf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            kafkaConf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            checkTaskFuture = checkScheduledExecutor.scheduleWithFixedDelay(this::checkDelayLoss,5,1, TimeUnit.SECONDS);
            logger.info("kafka conf init over");

        } catch (Exception e) {
            logger.warn("unexpected exception occur in init kafka conf", e);
        }
    }

    private boolean startConsume() {
        if (kafkaConsumer == null) {
            logger.warn("kafkaConsumer is null");
            return false;
        }
        logger.info("kafka consumer start consuming");
        future = kafkaConsumeService.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(200));
                    for (ConsumerRecord<String, String> mqRecord : records) {
                        executorService.submit(() -> processMessage(mqRecord));
                    }
                    if (!records.isEmpty()) {
                        kafkaConsumer.commitSync();
                    }
                } catch (KafkaException e) {
                    logger.warn("[[monitor=delay,mqType=kafka]] consumer exception:", e);
                    kafkaConsumer.seekToEnd(kafkaConsumer.assignment());
                }
            }
        });
        return true;
    }

    private void processMessage(ConsumerRecord<String, String> record) {
        logger.info("[[monitor=delay,mqType=kafka]] consumer partition: {}, offset: {}", record.partition(), record.offset());
        long receiveTime = System.currentTimeMillis();
        DataChangeMessage dataChange = JsonUtils.fromJson(record.value(), DataChangeMessage.class);
        if (EventType.UPDATE.name().equals(dataChange.getEventType())) {
            List<DataChangeMessage.ColumnData> afterColumnList = dataChange.getAfterColumnList();
            DataChangeMessage.ColumnData dcColumn = afterColumnList.get(DC_INDEX);
            DataChangeMessage.ColumnData delayInfoColumn = afterColumnList.get(DELAY_INFO_INDEX);
            DataChangeMessage.ColumnData timeColumn = afterColumnList.get(DATA_CHANGE_TIME_INDEX);

            String dc = dcColumn.getValue();
            String mhaInfoJson = delayInfoColumn.getValue();
            String mhaName;
            try {
                DelayInfo delayInfo = JsonUtils.fromJson(mhaInfoJson, DelayInfo.class);
                mhaName = delayInfo.getM();
            } catch (Exception e) {
                mhaName = mhaInfoJson;// for old version column compatible
            }

            if (!dcsRelated.contains(dc.toLowerCase())) {
                return;
            }
            if (!mhasRelated.contains(mhaName)) {
                logger.warn("[[monitor=delay,mqType=kafka]] not contains mha: {}", mhaName);
                return;
            }
            Pair<Integer, Long> lastProcessRecordPair = mhaLastReceiveMap.computeIfAbsent(mhaName, (key) -> Pair.of(-1,-1L));
            Pair<Integer, Long> incomingMessagePair = Pair.of(record.partition(), record.offset());
            if (lastProcessRecordPair.equals(incomingMessagePair)) {
                return;
            }

            MhaInfo mhaInfo = new MhaInfo(mhaName, dc);
            Timestamp updateDbTime = Timestamp.valueOf(timeColumn.getValue());
            long delayTime = receiveTime - updateDbTime.getTime();

            DefaultReporterHolder.getInstance().reportMessengerDelay(
                    mhaInfo.getTags(), delayTime, "fx.drc.messenger.delay");
            logger.info("[[monitor=delay,mha={},mqType=kafka,partition={},offset={}]] receiveTime:{}, updateDbTime:{}, report messenger delay:{} ms", mhaName, record.partition(), record.offset(), receiveTime, updateDbTime.getTime(), delayTime);

            receiveTimeMap.put(mhaInfo,receiveTime);

            mhaLastReceiveMap.put(mhaName, incomingMessagePair);
        } else {
            logger.info("[[monitor=delay,mqType=kafka]] discard delay monitor message which is not update");
        }
    }

    private void checkDelayLoss() {

        for (String mhaName : mhasRelated) {
            MhaInfo mhaInfo = new MhaInfo(mhaName, mha2Dc.get(mhaName));
            Long receiveTime = receiveTimeMap.putIfAbsent(mhaInfo, System.currentTimeMillis());
            if (receiveTime == null) {
                continue;
            }
            long curTime = System.currentTimeMillis();
            long timeDiff = curTime - receiveTime;
            if (timeDiff > TOLERANCE_TIME) {
                logger.error("[[monitor=delay,mqType=kafka]] mha:{}, delayMessageLoss ,curTime:{}, receiveTime:{}, report Huge to trigger alarm", mhaInfo.getMhaName(), curTime, receiveTime);
                DefaultReporterHolder.getInstance()
                        .reportMessengerDelay(mhaInfo.getTags(), HUGE_VAL, MQ_DELAY_MEASUREMENT);
            }
        }
    }

    @Override
    public void mhasRefresh(Map<String, String> mha2Dc) {
      synchronized (this) {
          logger.info("[KafkaDelayMessageConsumer] mhasRefresh: {}",mha2Dc);
          Set<String> mhas = Sets.newHashSet(mha2Dc.keySet());
          Set<String> deleteMhas = Sets.newHashSet(mhasRelated);
          deleteMhas.removeAll(mhas);
          for (String mhaName : deleteMhas) {
              String dc = this.mha2Dc.get(mhaName);
              logger.info("[KafkaDelayMessageConsumer] remove mha: {}, {}", mhaName, dc);
              if (dc == null) {
                  if (this.mhasRelated.contains(mhaName)) {
                      logger.warn("[KafkaDelayMessageConsumer] mhasRefresh error mha: {}", mhaName);
                  }
                  continue;
              }
              MhaInfo mhaInfo = new MhaInfo(mhaName, dc);
              boolean res = DefaultReporterHolder.getInstance().removeRegister(MQ_DELAY_MEASUREMENT, mhaInfo.getTags());
              logger.info("[KafkaDelayMessageConsumer] removeRegister: {}, {}, {}", mhaName, dc, res);
          }
          this.mhasRelated = mhas;
          this.mha2Dc = mha2Dc;
      }
    }

    @Override
    public void addMhas(Map<String, String> mhas2Dc) {
        logger.info("[KafkaDelayMessageConsumer] addMhas: {}", mhas2Dc);
        synchronized (this) {
            this.mhasRelated.addAll(mhas2Dc.keySet());
            this.mha2Dc.putAll(mhas2Dc);
        }

    }

    @Override
    public void removeMhas(Map<String, String> mhas2Dc) {
        logger.info("[KafkaDelayMessageConsumer] removeMhas: {}", mhas2Dc);
        synchronized (this) {
            for (String mhaName : mhas2Dc.keySet()) {
                String dc = this.mha2Dc.remove(mhaName);
                logger.info("[KafkaDelayMessageConsumer] remove mha: {}, {}", mhaName, dc);
                if (dc == null && this.mhasRelated.contains(mhaName)) {
                    logger.warn("[KafkaDelayMessageConsumer] error mha: {}", mhaName);
                }
                if (dc != null) {
                    MhaInfo mhaInfo = new MhaInfo(mhaName, dc);
                    boolean res = DefaultReporterHolder.getInstance().removeRegister(MQ_DELAY_MEASUREMENT, mhaInfo.getTags());
                    logger.info("[KafkaDelayMessageConsumer] removeRegister: {}, {}, {}", mhaName, dc, res);
                }
            }
            this.mhasRelated.removeAll(mhas2Dc.keySet());
        }
    }

    @Override
    public boolean stopConsume() {
        logger.info("[KafkaDelayMessageConsumer] stopConsume Kafka");
        if (future == null || checkTaskFuture == null) {
            return false;
        }
        checkTaskFuture.cancel(true);
        future.cancel(true);
        receiveTimeMap.clear();
        mhaLastReceiveMap.clear();
        DefaultReporterHolder.getInstance().removeRegister(MQ_DELAY_MEASUREMENT);
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
        return true;
    }

    @Override
    public boolean resumeConsume() {
        logger.info("[KafkaDelayMessageConsumer] resumeConsume Kafka");
        if (kafkaConf == null) {
            return false;
        }

        if (future != null && !future.isCancelled() && !future.isDone()) {
            return false;
        }

        if (checkTaskFuture == null || checkTaskFuture.isCancelled() || checkTaskFuture.isDone()) {
            checkTaskFuture = checkScheduledExecutor.scheduleWithFixedDelay(this::checkDelayLoss,5,1, TimeUnit.SECONDS);
        }
        try {
            kafkaConsumer = KafkaClientFactory.newConsumer(subject, consumerGroup, kafkaConf);
        } catch (IOException e) {
            logger.error("kafka consumer init error", e);
        }
        logger.info("kafka consumer init over");

        return startConsume();
    }

    @Override
    public int getOrder() {
        return 0;
    }

    private static class MhaInfo {

        private Map<String, String> tags;
        private String mhaName;
        private String dc;


        public Map<String,String> getTags() {
            if (tags == null) {
                tags = Maps.newHashMap();
                tags.put("mhaName",mhaName);
                tags.put("dc",dc);
                tags.put("mqType", MqType.kafka.name());
            }
            return tags;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MhaInfo mhaInfo = (MhaInfo) o;
            return Objects.equals(mhaName, mhaInfo.mhaName) && Objects.equals(dc, mhaInfo.dc);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mhaName, dc);
        }

        public MhaInfo(String mhaName, String dc) {
            this.mhaName = mhaName;
            this.dc = dc;
        }

        public String getMhaName() {
            return mhaName;
        }

        public void setMhaName(String mhaName) {
            this.mhaName = mhaName;
        }

        public String getDc() {
            return dc;
        }

        public void setDc(String dc) {
            this.dc = dc;
        }
    }
}
