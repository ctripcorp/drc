package com.ctrip.framework.drc.console.monitor.delay;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.v2.CentralService;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.mq.DelayMessageConsumer;
import com.ctrip.framework.drc.core.server.DcLeaderAware;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.HashMap;
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
public class MqDelayMonitorServer implements DcLeaderAware, InitializingBean {

    @Autowired
    private MonitorTableSourceProvider monitorProvider;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private MetaProviderV2 metaProviderV2;
    @Autowired
    private CentralService centralService;

    private final DelayMessageConsumer consumer = ApiContainer.getDelayMessageConsumer();
    private final ScheduledExecutorService monitorMessengerChangerExecutor = ThreadUtils.newSingleThreadScheduledExecutor(
            getClass().getSimpleName() + "messengerMonitor");

    private static final Logger logger = LoggerFactory.getLogger("delayMonitorLogger");

    private volatile boolean isLeader = false;

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("[[monitor=qmqDelay]] mq consumer starting");
        monitorMessengerChangerExecutor.scheduleWithFixedDelay(this::monitorMessengerChange, 5, 30, TimeUnit.SECONDS);
        consumer.initConsumer(
                monitorProvider.getMqDelaySubject(),
                monitorProvider.getMqDelayConsumerGroup(),
                consoleConfig.getDcsInLocalRegion()
        );
    }

    public void monitorMessengerChange() {
        if (isLeader) {
            try {
                logger.info("[[monitor=qmqDelay]] start monitorMessengerChange");
                consumer.mhasRefresh(getAllMhasRelated());
            } catch (Throwable t) {
                logger.error("[[monitor=qmqDelay]] monitorMessengerChange fail", t);
            }
        } else {
            consumer.mhasRefresh(new HashMap<>());
        }
    }


    private Map<String, String> getAllMhasRelated() throws SQLException {
        Set<String> mhas = getAllMhaWithMessengerInLocalRegion();
        return getMhasToDcs(Lists.newArrayList(mhas));
    }

    @VisibleForTesting
    protected Set<String> getAllMhaWithMessengerInLocalRegion() { // mqType -> Pair<mhaName, dc>
        Set<String> mhas = Sets.newHashSet();
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
                if (existQmQMessenger) {
                    mhas.add(dbCluster.getMhaName());
                }
            }
        }
        return mhas;
    }

    private Map<String, String> getMhasToDcs(List<String> mhaNames) throws SQLException {
        List<MhaTblV2> mhas = centralService.queryAllMhaTblV2().stream().filter(e -> mhaNames.contains(e.getMhaName())).toList();
        Map<Long, String> dcMap = centralService.queryAllDcTbl().stream().collect(Collectors.toMap(DcTbl::getId, DcTbl::getDcName));

        return mhas.stream().collect(Collectors.toMap(MhaTblV2::getMhaName, e -> dcMap.get(e.getDcId())));
    }

    @Override
    public void isleader() {
        synchronized (this) {
            if ("on".equalsIgnoreCase(monitorProvider.getMqDelayMonitorSwitch())) {
                isLeader = true;
                monitorMessengerChange();
                boolean res = consumer.resumeListen();
                logger.info("[[monitor=qmqDelay]] is leader,going to start qmqConsumer,result:{}", res);
            }
        }


    }

    @Override
    public void notLeader() {
        synchronized (this) {
            if ("on".equalsIgnoreCase(monitorProvider.getMqDelayMonitorSwitch())) {
                isLeader = false;
                monitorMessengerChange();
                boolean res = consumer.stopListen();
                logger.info("[[monitor=qmqDelay]] not leader,going to stop qmqConsumer,result:{}", res);
            }
        }
    }

}
