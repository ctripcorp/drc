package com.ctrip.framework.drc.monitor.function.cases.messenger;

import com.ctrip.datasource.message.BinlogMessage;
import com.ctrip.framework.dal.cluster.client.message.NonLocalBinlogConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Created by jixinwang on 2022/11/2
 */
@Service
public class MessengerConsumerCase {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private void manualCreateConsumer() {

    }

    @NonLocalBinlogConsumer("bbz.drc.delaymonitor")
    public void testNonLocalBinlog(BinlogMessage msg) throws InterruptedException {
        System.out.println(msg.getDataChange().getBeforeColumnList());
        System.out.println(msg.getDataChange());
    }
}
