package com.ctrip.framework.drc.manager.healthcheck.notifier;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

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
        for (Messenger messenger : dbCluster.getMessengers()) {
            if (ipAndPort.equals(messenger.getIp() + ":" + messenger.getPort())) {
                config.ip = messenger.getIp();
                config.port = messenger.getPort();
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
