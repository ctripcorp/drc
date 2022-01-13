package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.service.ModuleCommunicationService;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static com.ctrip.xpipe.spring.AbstractController.CLUSTER_ID_PATH_VARIABLE;

@Service
public class ModuleCommunicationServiceImpl implements ModuleCommunicationService {

    private static final Logger logger = LoggerFactory.getLogger(ModuleCommunicationServiceImpl.class);

    public static final String PATH_PREFIX = "/api/meta";

    public static final String GET_ACTIVE_REPLICATOR = "/getactivereplicator/" + CLUSTER_ID_PATH_VARIABLE + "/";

    public static final String GET_ACTIVE_MYSQL = "/getactivemysql/" + CLUSTER_ID_PATH_VARIABLE + "/";

    @Autowired
    private DefaultConsoleConfig config;

    @Override
    public Replicator getActiveReplicator(String dc, String clusterId) {
        try {
            String cmMetaServerAddress = config.getCMMetaServerAddress(dc);
            if (StringUtils.isNotBlank(cmMetaServerAddress)) {
                String activeReplicatorPath = getActiveReplicatorPath(cmMetaServerAddress);
                return HttpUtils.get(activeReplicatorPath, Replicator.class, clusterId);
            }
        } catch(Exception e) {
            logger.error("[[registryKey={},dc={}]]getActiveReplicator, ", clusterId, dc, e);
        }
        return null;
    }

    @Override
    public Endpoint getActiveMySQL(String dc, String clusterId) {
        String cmMetaServerAddress = config.getCMMetaServerAddress(dc);
        String activeMySQLPath = getActiveMySQLPath(cmMetaServerAddress);
        return HttpUtils.get(activeMySQLPath, DefaultEndPoint.class, clusterId);
    }



    private static String getActiveMySQLPath(String host) {
        if (!host.startsWith("http")) {
            host = "http://" + host;
        }
        return String.format("%s%s%s", host, PATH_PREFIX, GET_ACTIVE_MYSQL);
    }

    private static String getActiveReplicatorPath(String host) {
        if (!host.startsWith("http")) {
            host = "http://" + host;
        }
        return String.format("%s%s%s", host, PATH_PREFIX, GET_ACTIVE_REPLICATOR);
    }
}
