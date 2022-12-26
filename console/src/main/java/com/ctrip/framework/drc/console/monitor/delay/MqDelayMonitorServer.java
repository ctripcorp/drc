package com.ctrip.framework.drc.console.monitor.delay;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.core.mq.DelayMessageConsumer;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.cluster.LeaderAware;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
    @Autowired private DbClusterSourceProvider dbClusterSource;
    @Autowired private DefaultConsoleConfig consoleConfig;

    private final DelayMessageConsumer consumer = ApiContainer.getDelayMessageConsumer();
    private final ScheduledExecutorService monitorMessengerChangerExecutor = ThreadUtils.newSingleThreadScheduledExecutor(
            getClass().getSimpleName() + "messengerMonitor");

    private static final Logger logger = LoggerFactory.getLogger("delayMonitorLogger");
    
    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("[[monitor=delay]] mq consumer starting");
        monitorMessengerChangerExecutor.scheduleWithFixedDelay(this::monitorMessengerChange,5,30, TimeUnit.SECONDS);
        consumer.initConsumer(
                monitorProvider.getMqDelaySubject(),
                monitorProvider.getMqDelayConsumerGroup(),
                consoleConfig.getDcsInLocalRegion()
        );
        
    }
    
    public void monitorMessengerChange() {
        try {
            Set<String> mhas = dbClusterSource.getAllMhaWithMessengerInLocalRegion();
            consumer.mhasRefresh(mhas);
        } catch (Throwable t) {
            logger.error("[[monitor=delay]] monitorMessengerChange fail",t);
        }
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
    
}
