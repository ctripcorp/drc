package com.ctrip.framework.drc.console.monitor.delay.server;

import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.core.mq.DelayMessageConsumer;
import com.ctrip.xpipe.api.cluster.LeaderAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @ClassName MqDelayMonitorServer
 * @Author haodongPan
 * @Date 2022/11/10 16:19
 * @Version: $
 */
@Order(1)
@Component
public class MqDelayMonitorServer implements LeaderAware {

    private static final Logger logger = LoggerFactory.getLogger("delayMonitorLogger");
    
    private DelayMessageConsumer consumer = ApiContainer.getDelayMessageConsumer();
    
    @PostConstruct
    public void init() {
        logger.info("[[monitor=delay]] mq consumer starting");
        consumer.initConsumer();
    }


    @Override
    public void isleader() {
        boolean b = consumer.resumeListen();
        logger.info("[[monitor=delay]] is leader,going to monitor messenger delayTime,result:{}",b);
    }

    @Override
    public void notLeader() {
        boolean b = consumer.stopListen();
        logger.info("[[monitor=delay]] not leader,stop monitor messenger delayTime,result:{}",b);
    }
}
