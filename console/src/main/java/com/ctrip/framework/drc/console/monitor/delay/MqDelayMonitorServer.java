package com.ctrip.framework.drc.console.monitor.delay;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.mq.DelayMessageConsumer;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;




/**
 * @ClassName MqDelayMonitorServer
 * @Author haodongPan
 * @Date 2022/11/10 16:19
 * @Version: $
 */
@Order(0)
@Component("mqDelayMonitorServer")
public class MqDelayMonitorServer implements InitializingBean {
    
    @Autowired private MonitorTableSourceProvider monitorProvider;
    @Autowired private DefaultConsoleConfig consoleConfig;
    @Autowired private MetaProviderV2 metaProviderV2;

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
            Set<String> mhas = this.getAllMhaWithMessengerInLocalRegion();
            consumer.mhasRefresh(mhas);
        } catch (Throwable t) {
            logger.error("[[monitor=delay]] monitorMessengerChange fail",t);
        }
    }


    @VisibleForTesting
    Set<String> getAllMhaWithMessengerInLocalRegion() {
        Set<String> res = Sets.newHashSet();
        Set<String> dcsInLocalRegion = consoleConfig.getDcsInLocalRegion();
        Drc drc = metaProviderV2.getDrc();

        for (String dcInLocalRegion : dcsInLocalRegion) {
            Dc dc = drc.findDc(dcInLocalRegion);
            if (dc == null) {
                continue;
            }
            for (DbCluster dbCluster : dc.getDbClusters().values()) {
                List<Messenger> messengers = dbCluster.getMessengers();
                if (!messengers.isEmpty()) {
                    res.add(dbCluster.getMhaName());
                }
            }
        }
        return res;
    }

}
