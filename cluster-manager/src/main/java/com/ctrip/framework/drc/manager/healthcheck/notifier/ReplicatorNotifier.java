package com.ctrip.framework.drc.manager.healthcheck.notifier;


import com.ctrip.framework.drc.core.driver.config.GlobalConfig;
import com.ctrip.framework.drc.core.driver.config.InstanceStatus;
import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dbs;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorConfigDto;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.xpipe.foundation.DefaultFoundationService;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.NOTIFY_LOGGER;

/**
 * Created by mingdongli
 * 2019/11/22 上午12:34.
 */
public class ReplicatorNotifier extends AbstractNotifier implements Notifier {

    private static final String URL_PATH = "replicators";

    private ReplicatorNotifier() {
        super();
    }

    private static class ReplicatorNotifierHolder {
        public static final ReplicatorNotifier INSTANCE = new ReplicatorNotifier();
    }

    public static ReplicatorNotifier getInstance() {
        return ReplicatorNotifierHolder.INSTANCE;
    }

    @Override
    protected String getUrlPath() {
        return URL_PATH;
    }

    @Override
    protected Object getBody(String ipAndPort, DbCluster dbCluster, boolean register) {
        ReplicatorConfigDto configDto = new ReplicatorConfigDto();
        //monitor and register info
        configDto.setBu(dbCluster.getBuName());
        configDto.setClusterAppId(dbCluster.getAppId());
        configDto.setMhaName(dbCluster.getMhaName());
        configDto.setSrcDcName(System.getProperty(DefaultFoundationService.DATA_CENTER_KEY, GlobalConfig.DC));

        Replicator masterReplicator = dbCluster.getReplicators().stream().filter(rep -> rep.isMaster()).findFirst().orElse(dbCluster.getReplicators().get(0));
        Replicator notifyReplicator = dbCluster.getReplicators().stream().filter(rep -> ipAndPort.equalsIgnoreCase(rep.getIp() + ":" + rep.getPort())).findFirst().orElse(dbCluster.getReplicators().get(0));
        configDto.setClusterName(dbCluster.getName());
        configDto.setGtidSet(notifyReplicator.getGtidSkip());

        Dbs dbs = dbCluster.getDbs();
        List<Db> dbList = dbs.getDbs();
        List<String> uuids = Lists.newArrayList();
        Db dbMaster = new Db();

        for (Db db : dbList) {
            if (db.isMaster()) {
                dbMaster = MetaClone.clone(db);
            }
            String uuidString = db.getUuid();
            uuids.addAll(split(uuidString));
        }

        NOTIFY_LOGGER.info("[ipAndPort] {}, masterReplicator {}, notifyReplicator {}", ipAndPort, masterReplicator, notifyReplicator);
        if (ipAndPort.equalsIgnoreCase(masterReplicator.getIp() + ":" + masterReplicator.getPort())) {
            configDto.setMaster(dbMaster);
            configDto.setStatus(InstanceStatus.ACTIVE.getStatus());
            configDto.setApplierPort(masterReplicator.getApplierPort());
        } else {
            dbMaster.setIp(masterReplicator.getIp());
            dbMaster.setPort(masterReplicator.getApplierPort());
            configDto.setMaster(dbMaster);
            configDto.setStatus(InstanceStatus.INACTIVE.getStatus());
            configDto.setApplierPort(notifyReplicator.getApplierPort());
        }

        configDto.setReadUser(dbs.getReadUser());
        configDto.setReadPassward(dbs.getReadPassword());
        configDto.setUuids(uuids);
        configDto.setTableNames(split(masterReplicator.getExcludedTables()));
        configDto.setPreviousMaster(dbs.getPreviousMaster());
        configDto.setApplyMode(dbCluster.getApplyMode());
        return configDto;
    }

    @Override
    protected List<String> getDomains(DbCluster dbCluster) {
        List<String> ipPorts = dbCluster.getReplicators().stream().map(rep -> rep.getIp() + ":" + rep.getPort()).collect(Collectors.toList());
        return ipPorts;
    }

    private List<String> split(String value) {
        List<String> res = Lists.newArrayList();
        if (StringUtils.isNotBlank(value)) {
            String[] elements = value.split(SystemConfig.COMMA);
            for (String ele : elements) {
                if (StringUtils.isNotBlank(ele) && !res.contains(ele.trim())) {
                    res.add(ele.trim());
                }
            }
        }
        return res;
    }
}
