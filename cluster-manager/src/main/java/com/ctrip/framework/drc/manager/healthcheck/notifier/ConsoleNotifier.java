package com.ctrip.framework.drc.manager.healthcheck.notifier;

import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.server.config.console.dto.DbEndpointDto;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @Author limingdong
 * @create 2020/3/4
 */
public class ConsoleNotifier extends AbstractNotifier implements Notifier {

    private static final String URL_PATH = "api/drc/v1/switch/clusters/%s";

    private String clusterName;

    private String domain;

    private ConsoleNotifier() {
        super();
    }

    private static class ConsoleNotifierHolder {
        public static final ConsoleNotifier INSTANCE = new ConsoleNotifier();
    }

    public static ConsoleNotifier getInstance() {
        return ConsoleNotifierHolder.INSTANCE;
    }

    @Override
    protected String getUrlPath() {
        return String.format(URL_PATH, clusterName);
    }

    @Override
    protected Object getBody(String ipAndPort, DbCluster dbCluster, boolean register) {
        DbEndpointDto configDto = new DbEndpointDto();

        Dbs dbs = dbCluster.getDbs();
        List<Db> dbList = dbs.getDbs();

        for (Db db : dbList) {
            if (db.isMaster()) {
                configDto.setIp(db.getIp());
                configDto.setPort(db.getPort());
                break;
            }
        }
        return configDto;
    }

    @Override
    protected List<String> getDomains(DbCluster dbCluster) {
        if (StringUtils.isBlank(domain)) {
            return Lists.newArrayList();
        }
        return Lists.newArrayList(domain);
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }
}
