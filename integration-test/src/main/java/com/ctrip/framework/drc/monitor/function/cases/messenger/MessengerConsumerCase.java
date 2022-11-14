package com.ctrip.framework.drc.monitor.function.cases.messenger;

//import com.ctrip.datasource.message.BinlogMessage;
//import com.ctrip.framework.dal.cluster.client.message.NonLocalBinlogConsumer;
//import com.ctrip.framework.vi.server.VIServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

/**
 * Created by jixinwang on 2022/11/2
 */
@Service
public class MessengerConsumerCase {

//    private Logger logger = LoggerFactory.getLogger(getClass());
//
//    @PostConstruct
//    public void init() throws Exception {
//        VIServer viServer = new VIServer(9999);//Listener 模式需要启动VI;
//        viServer.start();
//    }
//
//    private void manualCreateConsumer() {
//
//    }
//
//    @NonLocalBinlogConsumer("bbz.drc.delaymonitor")
//    public void testNonLocalBinlog(BinlogMessage msg) throws InterruptedException {
//        System.out.println(msg.getDataChange().getBeforeColumnList());
//        System.out.println(msg.getDataChange());
//    }
}
