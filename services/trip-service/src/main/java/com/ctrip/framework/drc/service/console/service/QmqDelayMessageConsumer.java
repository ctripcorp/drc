package com.ctrip.framework.drc.service.console.service;


import com.ctrip.framework.drc.core.monitor.column.DelayInfo;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.mq.*;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.drc.service.mq.DataChangeMessage;
import com.ctrip.framework.drc.service.mq.DataChangeMessage.ColumnData;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import qunar.tc.qmq.*;
import qunar.tc.qmq.consumer.MessageConsumerProvider;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @ClassName QmqDelayConsumerService
 * @Author haodongPan
 * @Date 2022/10/27 20:58
 * @Version: $
 */
public class QmqDelayMessageConsumer implements DelayMessageConsumer {

    private static final Logger logger = LoggerFactory.getLogger("delayMonitorLogger");

    private static final int DC_INDEX = 1;
    private static final int DELAY_INFO_INDEX = 2;
    private static final int DATA_CHANGE_TIME_INDEX = 3;
    private static final long TOLERANCE_TIME = 3 * 60000L;
    private static final long HUGE_VAL = 60 * 60000L;
    private static final String MQ_DELAY_MEASUREMENT = "fx.drc.messenger.delay";

    private ListenerHolder listenerHolder;

    private Set<String> dcsRelated = Sets.newHashSet();
    private volatile Set<String> mhasRelated = Sets.newHashSet();
    private volatile Map<String, String> mha2Dc = Maps.newConcurrentMap();

    // k: mhaInfo ,v :receiveTime
    private final Map<MhaInfo, Long> receiveTimeMap = Maps.newConcurrentMap();
    private Map<String, Long> mhaDelayFromOtherDc = Maps.newConcurrentMap();

    private final ScheduledExecutorService checkScheduledExecutor =
            ThreadUtils.newSingleThreadScheduledExecutor("MessengerDelayMonitor");

    @Override
    public void initConsumer(String subject, String consumerGroup, Set<String> dcs) {
        try {
            dcsRelated = dcs;
            SubscribeParam param = new SubscribeParam.SubscribeParamBuilder().
                    setTagType(TagType.AND).
                    setTags(Sets.newHashSet(DcTag.LOCAL.getName())).
                    setConsumeStrategy(ConsumeStrategy.SHARED).
                    create();
            MessageConsumerProvider consumerProvider = ConsumerProviderHolder.instance;
            consumerProvider.init();
            listenerHolder = consumerProvider.addListener(subject, consumerGroup, this::processMessage, param);
            checkScheduledExecutor.scheduleWithFixedDelay(this::checkDelayLoss, 5, 1, TimeUnit.SECONDS);
            stopListen();
            logger.info("qmq consumer init over, start to listen");
        } catch (Exception e) {
            logger.error("unexpected exception occur in initQmQConsumer", e);
        }
    }

    @Override
    public boolean stopListen() {
        if (listenerHolder == null) {
            return false;
        }
        listenerHolder.stopListen();
        initMhas();
        return true;
    }

    @Override
    public boolean resumeListen() {
        if (listenerHolder == null) {
            return false;
        }
        listenerHolder.resumeListen();
        initMhas();
        return true;
    }

    @Override
    public void mhasRefresh(Map<String, String> mha2Dc) {
        logger.info("[QmqDelayMessageConsumer] mhasRefresh: {}", mha2Dc);
        if (CollectionUtils.isEmpty(mha2Dc)) {
            initMhas();
            return;
        }

        Set<String> mhas = Sets.newHashSet(mha2Dc.keySet());
        Set<String> deleteMhas = Sets.newHashSet(mhasRelated);
        deleteMhas.removeAll(mhas);
        for (String mhaName : deleteMhas) {
            String dc = this.mha2Dc.get(mhaName);
            logger.info("[QmqDelayMessageConsumer] remove mha: {}, {}", mhaName, dc);
            if (dc == null && this.mhasRelated.contains(mhaName)) {
                logger.warn("[QmqDelayMessageConsumer] mhasRefresh error mha: {}", mhaName);
                continue;
            }
            MhaInfo mhaInfo = new MhaInfo(mhaName, dc, MqType.qmq.name());
            receiveTimeMap.remove(mhaInfo);
        }
        this.mhasRelated = mhas;
        this.mha2Dc = mha2Dc;
    }

    private void initMhas() {
        this.mhasRelated = Sets.newHashSet();
        this.mha2Dc.clear();
        this.receiveTimeMap.clear();
    }

    @Override
    public Map<String, Long> getMhaDelay() {
        Map<String, Long> delay = Maps.newHashMap();
        receiveTimeMap.forEach((mhaInfo, delayTime) -> {
            delay.put(mhaInfo.getMhaName(), delayTime);
        });
        return delay;
    }

    @Override
    public void refreshMhaDelayFromOtherDc(Map<String, Long> mhaDelayMap) {
        for (Map.Entry<String, Long> entry : mhaDelayMap.entrySet()) {
            String mhaName = entry.getKey();
            Long delay = entry.getValue();

            Long lastDelay = mhaDelayFromOtherDc.get(mhaName);
            if (lastDelay == null || delay > lastDelay) {
                mhaDelayFromOtherDc.put(mhaName, delay);
            }
        }
    }

    private void processMessage(Message message) {
        logger.info("[[monitor=delay,mqType=qmq]] consumer message: {}", message.getMessageId());
        long receiveTime = System.currentTimeMillis();
        String dataChangeJson = message.getStringProperty("dataChange");
        DataChangeMessage dataChange = JsonUtils.fromJson(dataChangeJson, DataChangeMessage.class);
        if (EventType.UPDATE.name().equals(dataChange.getEventType())) {
            List<DataChangeMessage.ColumnData> afterColumnList = dataChange.getAfterColumnList();
            ColumnData dcColumn = afterColumnList.get(DC_INDEX);
            ColumnData delayInfoColumn = afterColumnList.get(DELAY_INFO_INDEX);
            ColumnData timeColumn = afterColumnList.get(DATA_CHANGE_TIME_INDEX);

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

            MhaInfo mhaInfo = new MhaInfo(mhaName, dc, MqType.qmq.name());
            Timestamp updateDbTime = Timestamp.valueOf(timeColumn.getValue());
            long delayTime = receiveTime - updateDbTime.getTime();
            DefaultReporterHolder.getInstance().reportResetTimer(
                    mhaInfo.getTags(), delayTime, MQ_DELAY_MEASUREMENT);
            logger.info("[[monitor=delay,mha={},mqType=qmq,messageId={}]] receiveTime:{}, updateDbTime:{}, report messenger delay:{} ms", mhaName, message.getMessageId(), receiveTime, updateDbTime.getTime(), delayTime);

            receiveTimeMap.put(mhaInfo, receiveTime);
        } else {
            logger.info("[[monitor=delay,mqType=qmq]] discard delay monitor message which is not update");
        }
    }


    private void checkDelayLoss() {
        for (String mhaName : mhasRelated) {
            MhaInfo mhaInfo = new MhaInfo(mhaName, mha2Dc.get(mhaName), MqType.qmq.name());
            Long receiveTime = receiveTimeMap.putIfAbsent(mhaInfo, System.currentTimeMillis());
            if (receiveTime == null) {
                continue;
            }
            long curTime = System.currentTimeMillis();
            long timeDiff = curTime - receiveTime;

            if (timeDiff < TOLERANCE_TIME) {
                continue;
            }
            Long receiveTimeFromOtherDc = mhaDelayFromOtherDc.get(mhaName);
            if (receiveTimeFromOtherDc != null && curTime - receiveTimeFromOtherDc < TOLERANCE_TIME) {
                logger.info("[[monitor=delay,mqType=qmq]] receive delay from other dc, mha:{}, ,curTime:{}, receiveTime:{}", mhaInfo.getMhaName(), curTime, receiveTime);
                continue;
            }
            logger.error("[[monitor=delay,mqType=qmq]] mha:{}, delayMessageLoss ,curTime:{}, receiveTime:{}, report Huge to trigger alarm", mhaInfo.getMhaName(), curTime, receiveTime);
            DefaultReporterHolder.getInstance().reportResetTimer(mhaInfo.getTags(), HUGE_VAL, MQ_DELAY_MEASUREMENT);
        }
    }

    @Override
    public int getOrder() {
        return 0;
    }


    private static class ConsumerProviderHolder {
        private static final MessageConsumerProvider instance = new MessageConsumerProvider();
    }


}
