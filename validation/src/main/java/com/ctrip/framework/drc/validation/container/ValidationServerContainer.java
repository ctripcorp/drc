package com.ctrip.framework.drc.validation.container;

import com.ctrip.framework.drc.core.server.config.validation.dto.ValidationConfigDto;
import com.ctrip.framework.drc.validation.server.ValidationServer;
import com.ctrip.framework.drc.validation.server.ValidationServerInCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class ValidationServerContainer {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected ConcurrentHashMap<String, ValidationServerInCluster> servers
            = new ConcurrentHashMap<>();

    public boolean addServer(ValidationConfigDto config) throws Exception {
        String clusterKey = config.getRegistryKey();
        if (servers.containsKey(clusterKey)) {
            logger.info("validation servers already contain {}, will check config", clusterKey);
            ValidationServerInCluster activeServer = servers.get(clusterKey);
            if (activeServer.config.equals(config)) {
                logger.info("old config equals new config: {}", config);
                return false;
            } else {
                logger.info("same cluster, new config received, going to stop & dispose old validation server: " +
                        "\n" + activeServer.config + "\nthen initialize & start new validation server: \n" + config);
                doRemoveServer(activeServer);
                doAddServer(config);
                return true;
            }
        }

        logger.info("validation servers does not contain {}, do add server", clusterKey);
        doAddServer(config);
        return true;
    }

    protected void doAddServer(ValidationConfigDto config) throws Exception {
        String clusterKey = config.getRegistryKey();
        logger.info("start to add validation servers for {}", clusterKey);
        ValidationServerInCluster newServer = new ValidationServerInCluster(config);
        logger.info("new ValidationServerInCluster finished for {}", clusterKey);
        newServer.initialize();
        newServer.start();
        logger.info("validation servers put cluster: {}", clusterKey);
        servers.put(clusterKey, newServer);
    }

    private void doRemoveServer(ValidationServer server) throws Exception {
        if (server != null) {
            server.stop();
            server.dispose();
        }
    }

    public void removeServer(String registryKey) throws Exception {
        ValidationServer server = servers.remove(registryKey);
        doRemoveServer(server);
    }
}
