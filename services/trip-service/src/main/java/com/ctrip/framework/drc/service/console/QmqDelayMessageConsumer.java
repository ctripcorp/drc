package com.ctrip.framework.drc.service.console;


import com.ctrip.framework.drc.core.monitor.column.DelayInfo;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.mq.DcTag;
import com.ctrip.framework.drc.core.mq.DelayMessageConsumer;
import com.ctrip.framework.drc.core.mq.EventType;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.drc.service.mq.DataChangeMessage;
import com.ctrip.framework.drc.service.mq.DataChangeMessage.ColumnData;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.*;
import qunar.tc.qmq.consumer.MessageConsumerProvider;


import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private static final long TOLERANCE_TIME = 5 * 60000L;
    private static final long HUGE_VAL = 60 * 60000L;
    private static final String MQ_DELAY_MEASUREMENT = "fx.drc.messenger.delay";

    private ListenerHolder listenerHolder;
    
    private Set<String> dcsRelated = Sets.newHashSet();
    private volatile Set<String> mhasRelated = Sets.newHashSet();
    
    // k: mhaInfo ,v :receiveTime
    private final Map<MhaInfo,Long> receiveTimeMap = Maps.newConcurrentMap();
    private final ScheduledExecutorService checkScheduledExecutor = 
            ThreadUtils.newSingleThreadScheduledExecutor("MessengerDelayMonitor");
    
    @Override
    public void initConsumer(String subject, String consumerGroup, Set<String> dcs){
        try {
            dcsRelated = dcs;
            SubscribeParam param = new SubscribeParam.SubscribeParamBuilder().
                    setTagType(TagType.AND).
                    setTags(Sets.newHashSet(DcTag.LOCAL.getName())).
                    create();
            MessageConsumerProvider consumerProvider = ConsumerProviderHolder.instance;
            consumerProvider.init();
            listenerHolder =  consumerProvider.addListener(subject, consumerGroup, this::processMessage, param);
            checkScheduledExecutor.scheduleWithFixedDelay(this::checkDelayLoss,5,1, TimeUnit.SECONDS);
            listenerHolder.stopListen();
            logger.info("qmq consumer init over,stop and wait for leadership gain");
        } catch (Exception e) {
            logger.error("unexpected exception occur in initConsumer",e);
        }
    }

    @Override
    public void mhasRefresh(Set<String> mhas) {
        mhasRelated = mhas;
    }

    @Override
    public boolean stopListen() {
        if (listenerHolder == null) {
            return false;
        }
        listenerHolder.stopListen();
        receiveTimeMap.clear();
        return true;
    }

    @Override
    public boolean resumeListen(){
        if (listenerHolder == null) {
            return false;
        }
        listenerHolder.resumeListen();
        return true;
    }

    private void processMessage(Message message) {
        logger.info("[[monitor=delay,mha={}]] consumer message " + message.getMessageId());
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
            
            MhaInfo mhaInfo = new MhaInfo(mhaName, dc);
            Timestamp updateDbTime = Timestamp.valueOf(timeColumn.getValue());
            long delayTime = receiveTime - updateDbTime.getTime();
            DefaultReporterHolder.getInstance().reportMessengerDelay(
                    mhaInfo.getTags(), delayTime, "fx.drc.messenger.delay");
            logger.info("[[monitor=delay,mha={}]] report messenger delay:{} ms", mhaName, delayTime);


            receiveTimeMap.put(mhaInfo,receiveTime);
        } else {
            logger.info("[[monitor=delay]] discard delay monitor message which is not update");
        }
    }


    private void checkDelayLoss() {
        for (Map.Entry<MhaInfo, Long> entry : receiveTimeMap.entrySet()) {
            MhaInfo mhaInfo = entry.getKey();
            String mhaName = mhaInfo.getMhaName();
            if (mhasRelated.contains(mhaName)) {
                long receiveTime = entry.getValue();
                long curTime = System.currentTimeMillis();
                long timeDiff = curTime - receiveTime;
                if (timeDiff > TOLERANCE_TIME) {
                    logger.error("[[monitor=delay]] mha:{}, delayMessageLoss, report Huge to trigger alarm",mhaInfo.getMhaName());
                    DefaultReporterHolder.getInstance()
                            .reportMessengerDelay(mhaInfo.getTags(), HUGE_VAL, MQ_DELAY_MEASUREMENT);
                }
            }
        }
    }

    @Override
    public int getOrder() {
        return 0;
    }


    private static class ConsumerProviderHolder {
        private static final MessageConsumerProvider instance = new MessageConsumerProvider();
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
