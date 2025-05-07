package com.ctrip.framework.drc.manager.healthcheck.notifier;

import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerConfigDto;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.foundation.DefaultFoundationService;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.*;

/**
 * Created by jixinwang on 2022/10/31
 */
public class MessengerNotifier extends AbstractNotifier implements Notifier {

    private static final String URL_PATH = "appliers";

    private MessengerNotifier() {
        super();
    }

    private static class MessengerNotifierHolder {
        public static final MessengerNotifier INSTANCE = new MessengerNotifier();
    }

    public static MessengerNotifier getInstance() {
        return MessengerNotifier.MessengerNotifierHolder.INSTANCE;
    }

    @Override
    protected String getUrlPath() {
        return URL_PATH;
    }

    @Override
    protected Object getBody(String ipAndPort, DbCluster dbCluster, boolean register) {
        MessengerConfigDto config = new MessengerConfigDto();
        config.setMhaName(dbCluster.getMhaName());
        config.cluster = dbCluster.getName();

        config.target = new DBInfo();
        Dbs dbs = dbCluster.getDbs();
        List<Db> dbList = dbs.getDbs();
        for (Db db : dbList) {
            if (db.isMaster()) {
                config.target.ip = db.getIp();
                config.target.port = db.getPort();
                config.target.uuid = db.getUuid();
                break;
            }
        }
        config.target.username = dbs.getWriteUser();
        config.target.password = dbs.getWritePassword();
        config.target.cluster = dbCluster.getName();
        config.target.mhaName = dbCluster.getMhaName();
        config.target.idc = System.getProperty(DefaultFoundationService.DATA_CENTER_KEY, "unknown");

        config.replicator = new InstanceInfo();

        Replicator replicator = null;
        List<Replicator> replicators = dbCluster.getReplicators();
        if (!replicators.isEmpty()) {
            replicator = replicators.get(0);
        }

        if (!register && replicator == null) {
            throw new RuntimeException("replicator not found.");
        } else {
            NOTIFY_LOGGER.info("notify - replicator: " + replicator);
        }

        if (!register) {
            config.replicator.ip = replicator.getIp();
            config.replicator.port = replicator.getApplierPort();
        }

        config.replicator.mhaName = getMessengerRegistryKeySuffix(dbCluster);

        for (Messenger messenger : dbCluster.getMessengers()) {
            if (ipAndPort.equals(getInstancesNotifyIpPort(messenger))) {
                config.ip = messenger.getIp();
                config.port = messenger.getPort();
                config.setGtidExecuted(messenger.getGtidExecuted());
                config.setIncludedDbs(messenger.getIncludedDbs());
                config.setApplyMode(messenger.getApplyMode());
                String nameFilter = messenger.getNameFilter();

                if (StringUtils.isBlank(nameFilter)) {
                    nameFilter = getDelayMonitorRegex(messenger.getApplyMode(), messenger.getIncludedDbs());
                } else {
                    String formatNameFilter = nameFilter.trim().toLowerCase();
                    if (!formatNameFilter.contains(DRC_DELAY_MONITOR_NAME) && !formatNameFilter.contains(DRC_DELAY_MONITOR_NAME_REGEX)) {
                        nameFilter = getDelayMonitorRegex(messenger.getApplyMode(), messenger.getIncludedDbs()) + "," + nameFilter;
                    }
                }
                config.setNameFilter(nameFilter);
                config.setProperties(messenger.getProperties());
            }
        }

        return config;
    }

    public String getMessengerRegistryKeySuffix(DbCluster dbCluster) {
        List<ApplyMode> applyModes = dbCluster.getMessengers().stream().map(e -> ApplyMode.getApplyMode(e.getApplyMode())).distinct().collect(Collectors.toList());
        if (applyModes.size() != 1) {
            EventMonitor.DEFAULT.logEvent("drc.cm.messenger.notify.mixedMode", dbCluster.getMhaName());
            logger.error("illegal operation: notify messengers with different apply mode: {}", dbCluster);
            throw new RuntimeException("illegal operation: notify messengers with different apply mode: " + dbCluster.getMhaName());
        }
        return MqType.parseByApplyMode(applyModes.get(0)).getRegistryKeySuffix();
    }

    @Override
    protected List<String> getDomains(DbCluster dbCluster) {
        if (dbCluster.getMessengers() == null) {
            return Lists.newArrayList();
        }
        return Lists.newArrayList(dbCluster.getMessengers().stream().map(
                this::getInstancesNotifyIpPort
        ).collect(Collectors.toList()));
    }
}
