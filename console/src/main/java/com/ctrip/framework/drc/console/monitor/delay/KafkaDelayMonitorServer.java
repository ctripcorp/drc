package com.ctrip.framework.drc.console.monitor.delay;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.monitor.delay.config.DataCenterService;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.v2.CentralService;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.mq.IKafkaDelayMessageConsumer;
import com.ctrip.framework.drc.core.server.DcLeaderAware;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerInfoDto;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2025/2/21 14:23
 */
@Order(0)
@Component()
public class KafkaDelayMonitorServer implements DcLeaderAware, InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger("delayMonitorLogger");

    @Autowired
    private DataCenterService dataCenterService;
    @Autowired
    private MonitorTableSourceProvider monitorProvider;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private MetaProviderV2 metaProviderV2;
    @Autowired
    private ResourceService resourceService;
    @Autowired
    private CentralService centralService;

    private final IKafkaDelayMessageConsumer kafkaConsumer = ApiContainer.getKafkaDelayMessageConsumer();
    private final ScheduledExecutorService monitorMessengerChangerExecutor = ThreadUtils.newSingleThreadScheduledExecutor(
            getClass().getSimpleName() + "messengerMonitor");

    private volatile boolean isLeader = false;

    private long localDcId;
    private String localDc;

    @Override
    public void afterPropertiesSet() throws Exception {
        localDc = dataCenterService.getDc();
        localDcId = centralService.queryAllDcTbl().stream().filter(e -> e.getDcName().equals(localDc)).findFirst().map(DcTbl::getId).orElse(0L);
        monitorMessengerChangerExecutor.scheduleWithFixedDelay(this::monitorMessengerChange, 5, 30, TimeUnit.SECONDS);

        kafkaConsumer.initConsumer(
                monitorProvider.getKafkaDelaySubject(),
                monitorProvider.getKafkaDelayConsumerGroup(),
                consoleConfig.getDcsInLocalRegion()
        );
    }

    @Override
    public void isleader() {
        synchronized (this) {
            isLeader = true;
            if ("on".equalsIgnoreCase(monitorProvider.getKafkaDelayMonitorSwitch())) {
                monitorMessengerChange();
                logger.info("[[monitor=kafkaDelay]] is leader,going to start kafkaConsumer");
                boolean b = kafkaConsumer.resumeConsume();
                logger.info("[[monitor=kafkaDelay]] is leader, start kafkaConsumer finished,result:{}", b);
            }
        }
    }

    @Override
    public void notLeader() {
        synchronized (this) {
            isLeader = false;
            if ("on".equalsIgnoreCase(monitorProvider.getKafkaDelayMonitorSwitch())) {
                monitorMessengerChange();
                logger.info("[[monitor=kafkaDelay]] not leader,going to stop kafkaConsumer");
                boolean b = kafkaConsumer.stopConsume();
                logger.info("[[monitor=kafkaDelay]] not leader, stop kafkaConsumer finished,result:{}", b);
            }
        }
    }

    private void monitorMessengerChange() {
        if (isLeader) {
            try {
                logger.info("[[monitor=kafkaDelay]] start monitorMessengerChange");
                long start = System.currentTimeMillis();
                Map<String, String> allMhasToDcRelated = getAllMhasToDcRelated();
                long end = System.currentTimeMillis();
                logger.info("[[monitor=kafkaDelay]] monitorMessengerChange end cost {} ms", end - start);
                kafkaConsumer.mhasRefresh(allMhasToDcRelated);
            } catch (Exception e) {
                logger.error("[[monitor=kafkaDelay]] monitorMessengerChange fail", e);
            }
        } else {
            kafkaConsumer.mhasRefresh(Maps.newHashMap());
        }
    }

    /**
     * @param mhaToMessengerIps key: mhaName, value: ip
     */
    public void switchListenMessenger(Map<String, String> mhaToMessengerIps) {
        if (!isLeader) {
            return;
        }
        try {
            logger.info("[[monitor=delay]] switchListenMessenger: {}", mhaToMessengerIps);
            List<String> localDcMessengerIps = centralService.queryAllResourceTbl().stream()
                    .filter(e -> e.getDcId() == localDcId && e.getType() == ModuleEnum.MESSENGER.getCode())
                    .map(ResourceTbl::getIp)
                    .toList();
            Set<String> toAddMhas = Sets.newHashSet();
            Set<String> toRemoveMhas = Sets.newHashSet();
            for (Map.Entry<String, String> entry : mhaToMessengerIps.entrySet()) {
                if (localDcMessengerIps.contains(entry.getValue())) {
                    toAddMhas.add(entry.getKey());
                } else {
                    toRemoveMhas.add(entry.getKey());
                }
            }
            if (!CollectionUtils.isEmpty(toAddMhas)) {
                kafkaConsumer.addMhas(getMhasToDcs(Lists.newArrayList(toAddMhas)));
            }
            if (!CollectionUtils.isEmpty(toRemoveMhas)) {
                kafkaConsumer.removeMhas(getMhasToDcs(Lists.newArrayList(toRemoveMhas)));
            }
        } catch (Exception e) {
            logger.error("[[monitor=kafkaDelay]] switchListenMessenger fail", e);
        }
    }

    private Map<String, String> getAllMhasToDcRelated() throws SQLException {
        List<String> localDcMessengerIps = centralService.queryAllResourceTbl().stream()
                .filter(e -> e.getDcId() == localDcId && e.getType() == ModuleEnum.MESSENGER.getCode())
                .map(ResourceTbl::getIp)
                .toList();
        Pair<List<String>, List<String>> pair = getAllMessengerIpsInLocalRegion();
        List<String> mhas = pair.getLeft();
        List<String> allMessengerIpsInLocalRegion = pair.getRight();

        List<MessengerInfoDto> messengersInAz = resourceService.getMasterMessengersInRegion(consoleConfig.getRegion(), allMessengerIpsInLocalRegion);
        List<String> localDcMhas = messengersInAz.stream()
                .filter(e -> RegistryKey.getTargetMha(e.getRegistryKey()).equals(SystemConfig.DRC_KAFKA) && localDcMessengerIps.contains(e.getIp()))
                .map(e -> RegistryKey.from(e.getRegistryKey()).getMhaName()).collect(Collectors.toList());
        List<String> localRegionMhas = messengersInAz.stream()
                .filter(e -> RegistryKey.getTargetMha(e.getRegistryKey()).equals(SystemConfig.DRC_KAFKA))
                .map(e -> RegistryKey.from(e.getRegistryKey()).getMhaName())
                .toList();

        mhas.removeAll(localRegionMhas);

        if (!CollectionUtils.isEmpty(mhas)) {
            logger.warn("[[monitor=kafkaDelay]] mhas has no master messenger, {}", mhas);
            localDcMhas.addAll(mhas);
        }

        return getMhasToDcs(localDcMhas);
    }

    private Map<String, String> getMhasToDcs(List<String> mhaNames) throws SQLException {
        List<MhaTblV2> mhas = centralService.queryAllMhaTblV2().stream().filter(e -> mhaNames.contains(e.getMhaName())).toList();
        Map<Long, String> dcMap = centralService.queryAllDcTbl().stream().collect(Collectors.toMap(DcTbl::getId, DcTbl::getDcName));

        return mhas.stream().collect(Collectors.toMap(MhaTblV2::getMhaName, e -> dcMap.get(e.getDcId())));
    }


    private Pair<List<String>, List<String>> getAllMessengerIpsInLocalRegion()  {
        Set<String> ips = new HashSet<>();
        List<String> mhas = new ArrayList<>();
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

                List<String> kafkaMessengerIps = messengers.stream().filter(e -> e.getApplyMode() == ApplyMode.kafka.getType()).map(Messenger::getIp).toList();
                if (CollectionUtils.isEmpty(kafkaMessengerIps)) {
                    continue;
                }
                ips.addAll(kafkaMessengerIps);
                mhas.add(dbCluster.getMhaName());
            }
        }
        return Pair.of(mhas, Lists.newArrayList(ips));
    }

}
