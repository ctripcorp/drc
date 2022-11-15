package com.ctrip.framework.drc.console.monitor.delay;

import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.core.mq.DelayMessageConsumer;
import com.ctrip.xpipe.api.cluster.LeaderAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;



/**
 * @ClassName MqDelayMonitorServer
 * @Author haodongPan
 * @Date 2022/11/10 16:19
 * @Version: $
 */
@Order(0)
@Component("mqDelayMonitorServer")
public class MqDelayMonitorServer implements LeaderAware, InitializingBean {
    
    @Autowired private MonitorTableSourceProvider monitorProvider;

    private static final Logger logger = LoggerFactory.getLogger("delayMonitorLogger");

    private final DelayMessageConsumer consumer = ApiContainer.getDelayMessageConsumer();
    
    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }

    @Override
    public void isleader() {
        if ("on".equalsIgnoreCase(monitorProvider.getMqDelayMonitorSwitch())) {
            boolean b = consumer.resumeListen();
            logger.info("[[monitor=delay]] is leader,going to monitor messenger delayTime,result:{}",b);
        }
    }
    
    @Override
    public void notLeader() {
        boolean b = consumer.stopListen();
        logger.info("[[monitor=delay]] not leader,stop monitor messenger delayTime,result:{}",b);
    }

    protected void init() {
        logger.info("[[monitor=delay]] mq consumer starting");
        consumer.initConsumer();
    }



}
