package com.ctrip.framework.drc.service.console;


import com.ctrip.framework.drc.core.monitor.column.DelayInfo;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.mq.DelayMessageConsumer;
import com.ctrip.framework.drc.core.mq.EventType;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.drc.service.mq.DataChangeMessage;
import com.ctrip.framework.drc.service.mq.DataChangeMessage.ColumnData;
import com.ctrip.framework.vi.server.VIServer;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.*;
import qunar.tc.qmq.consumer.MessageConsumerProvider;


import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;


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

    private ListenerHolder listenerHolder;
    
    @Override
    public void initConsumer(){
        try {
            VIServer viServer = new VIServer(9999);//Listener 模式需要启动VI;
            viServer.start();
            String subject = "bbz.drc.delaymonitor";
            String consumerGroup = "100023928";
            SubscribeParam param = new SubscribeParam.SubscribeParamBuilder().
                    setTagType(TagType.AND).
                    setTags(Sets.newHashSet("local")).
                    create();

            MessageConsumerProvider consumerProvider = ConsumerProviderHolder.instance;  
            consumerProvider.init();
            
            listenerHolder =  consumerProvider.addListener(subject, consumerGroup, this::processMessage, param);
        } catch (Exception e) {
           logger.error("unexpected exception occur",e);
        }
        
    }
    
    @Override
    public boolean stopListen() {
        if (listenerHolder == null) {
            return false;
        }
        listenerHolder.stopListen();
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
                // for old version column compatible
                mhaName = mhaInfoJson;
            }

            HashMap<String, String> tags = Maps.newHashMap();
            tags.put("mhaName", mhaName);
            tags.put("dc", dc);

            Timestamp updateDbTime = Timestamp.valueOf(timeColumn.getValue());
            long delayTime = receiveTime - updateDbTime.getTime();
            logger.info("[[monitor=delay,mha={}]] report messenger delay:{} ms", mhaName,
                    delayTime);
            DefaultReporterHolder.getInstance()
                    .reportMessengerDelay(tags, delayTime, "fx.drc.messenger.delay");

        } else {
            logger.info("[[monitor=delay]] discard delay monitor message which is not update");
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
