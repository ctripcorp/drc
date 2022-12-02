package com.ctrip.framework.drc.manager.healthcheck.notifier;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
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
        ApplierConfigDto config = new ApplierConfigDto();
        config.setMhaName(dbCluster.getMhaName());
        config.setApplyMode(ApplyMode.mq.getType());
        config.cluster = dbCluster.getName();

        config.target = new DBInfo();
        config.target.mhaName = dbCluster.getMhaName();

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

        config.replicator.mhaName = DRC_MQ;

        for (Messenger messenger : dbCluster.getMessengers()) {
            if (ipAndPort.equals(messenger.getIp() + ":" + messenger.getPort())) {
                config.ip = messenger.getIp();
                config.port = messenger.getPort();
                config.setGtidExecuted(messenger.getGtidExecuted());
                String nameFilter = messenger.getNameFilter();

                if (StringUtils.isNotBlank(nameFilter)) {
                    String formatNameFilter = nameFilter.trim().toLowerCase();
                    if (!formatNameFilter.contains(DRC_DELAY_MONITOR_NAME) && !formatNameFilter.contains(DRC_DELAY_MONITOR_NAME_REGEX)) {
                        nameFilter = DRC_DELAY_MONITOR_NAME_REGEX + "," + nameFilter;
                    }
                }
                config.setNameFilter(nameFilter);
                config.setProperties(messenger.getProperties());
            }
        }

        return config;
    }

    @Override
    protected List<String> getDomains(DbCluster dbCluster) {
        if (dbCluster.getMessengers() == null) {
            return Lists.newArrayList();
        }
        return Lists.newArrayList(dbCluster.getMessengers().stream().map(
                messenger -> messenger.getIp() + ":" + messenger.getPort()
        ).collect(Collectors.toList()));
    }
}
