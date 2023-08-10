package com.ctrip.framework.drc.console.monitor.delay.config.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.AbstractMonitor;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV2;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.PriorityOrdered;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName MetaProviderV2
 * @Author haodongPan
 * @Date 2023/7/10 14:53
 * @Version: $
 */
@Component
public class MetaProviderV2 extends AbstractMonitor implements PriorityOrdered  {
    
    @Autowired private MetaGeneratorV2 metaGeneratorV2;
    @Autowired private DefaultConsoleConfig consoleConfig;
    @Autowired private DbClusterSourceProvider metaProviderV1;

    protected volatile Drc drc;
    
    public synchronized Drc getDrc() {
        if (drc == null) {
           scheduledTask(); 
        }
        return drc;
    }

    public Dc getDcBy(String dbClusterId) {
        Drc drc = getDrc();
        Map<String, Dc> dcs = drc.getDcs();
        for (Entry<String, Dc> dcEntry : dcs.entrySet()) {
            Dc dc = dcEntry.getValue();
            DbCluster dbCluster = dc.findDbCluster(dbClusterId);
            if (dbCluster != null) {
                return dc;
            }
        }
        throw new IllegalArgumentException("dbCluster not find!");
    }

    public Endpoint getMasterEndpoint(String mha) {
        Map<String, Dc> dcs = getDcs();
        for(Dc dc : dcs.values()) {
            Map<String, DbCluster> dbClusters = dc.getDbClusters();
            DbCluster dbCluster = dbClusters.values().stream().filter(p -> mha.equalsIgnoreCase(p.getMhaName())).findFirst().orElse(null);
            if(null != dbCluster) {
                return getMaster(dbCluster);
            }
        }
        return null;
    }

    public Map<String, Dc> getDcs() {
        return getDrc() != null ? getDrc().getDcs() : Maps.newLinkedHashMap();
    }

    public Endpoint getMaster(DbCluster dbCluster) {
        Dbs dbs = dbCluster.getDbs();
        List<Db> dbList = dbs.getDbs();
        for(Db db : dbList) {
            if(db.isMaster()) {
                return new MySqlEndpoint(db.getIp(), db.getPort(), dbs.getMonitorUser(), dbs.getMonitorPassword(), BooleanEnum.TRUE.isValue());
            }
        }
        return null;
    }
    

    @Override
    public void initialize() {
        setInitialDelay(0);
        setPeriod(300);
        setTimeUnit(TimeUnit.SECONDS);
        super.initialize();
    }
    
    @Override // refresh when new config submit
    public synchronized void scheduledTask() {
        try {
            logger.info("[[meta=v2]] MetaProviderV2 start refresh drc");
            long start = System.currentTimeMillis();
            String region = consoleConfig.getRegion();
            Set<String> publicCloudRegion = consoleConfig.getPublicCloudRegion();
            if (publicCloudRegion.contains(region)) { // todo optimize,灰度阶段这么做，单写阶段需要修改
                drc = metaProviderV1.getDrc();
                return;
            }
            drc = metaGeneratorV2.getDrc();
            logger.info("[[meta=v2]] MetaProviderV2 refresh drc end cost:{}",System.currentTimeMillis() - start);
        } catch (Throwable t) {
            logger.error("[[meta=v2]] MetaProviderV2 get drc fail", t);
        }
        
    }

    @Override
    public int getOrder() {
        return 0;
    }

}
