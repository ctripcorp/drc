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
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.cluster.LeaderAware;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


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
    @Autowired private DefaultConsoleConfig consoleConfig;
    @Autowired private MetaProviderV2 metaProviderV2;

    private final DelayMessageConsumer consumer = ApiContainer.getDelayMessageConsumer();
    private final ScheduledExecutorService monitorMessengerChangerExecutor = ThreadUtils.newSingleThreadScheduledExecutor(
            getClass().getSimpleName() + "messengerMonitor");

    private static final Logger logger = LoggerFactory.getLogger("delayMonitorLogger");

    private volatile boolean isLeader = false;

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
            Map<String, Set<Pair<String, String>>> mqType2mhas = this.getAllMhaWithMessengerInLocalRegion();
            Set<String> qmqRelatedMhaNames = mqType2mhas.get(MqType.qmq.name()).stream()
                            .map(Pair::getLeft).collect(Collectors.toSet());
            consumer.mhasRefresh(qmqRelatedMhaNames);
        } catch (Throwable t) {
            logger.error("[[monitor=delay]] monitorMessengerChange fail",t);
        }
    }

    @VisibleForTesting
    Map<String, Set<Pair<String, String>>> getAllMhaWithMessengerInLocalRegion () { // mqType -> Pair<mhaName, dc>
        Map<String, Set<Pair<String, String>>> res = Arrays.stream(MqType.values()).sequential()
                        .collect(Collectors.toMap(
                                Enum::name,
                                mqType -> Sets.newHashSet()
                        ));

        Set<String> dcsInLocalRegion = consoleConfig.getDcsInLocalRegion();
        Drc drc = metaProviderV2.getDrc();
        for (String dcInLocalRegion : dcsInLocalRegion) {
            Dc dc = drc.findDc(dcInLocalRegion);
            if (dc == null) {
                continue;
            }
            for (DbCluster dbCluster : dc.getDbClusters().values()) {
                List<Messenger> messengers = dbCluster.getMessengers();
                if (messengers.isEmpty()) {
                    continue;
                }
                boolean existQmQMessenger = messengers.stream().anyMatch(m -> m.getApplyMode() == ApplyMode.mq.getType());
                boolean existKafkaMessenger = messengers.stream().anyMatch(m -> m.getApplyMode() == ApplyMode.kafka.getType());
                if (existQmQMessenger) {
                    res.get(MqType.qmq.name()).add(Pair.of(dbCluster.getMhaName(), dcInLocalRegion));
                }
                if (existKafkaMessenger) {
                    res.get(MqType.kafka.name()).add(Pair.of(dbCluster.getMhaName(), dcInLocalRegion));
                }
            }
        }
        return res;
    }

    @Override
    public void isleader() {
        isLeader = true;
    }

    @Override
    public void notLeader() {
        isLeader = false;
    }

}
