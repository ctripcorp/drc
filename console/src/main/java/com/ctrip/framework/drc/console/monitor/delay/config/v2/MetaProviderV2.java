package com.ctrip.framework.drc.console.monitor.delay.config.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.AbstractMonitor;
import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV2;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.PriorityOrdered;
import org.springframework.stereotype.Component;

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
    

    @Override
    public void initialize() {
        setInitialDelay(300);
        setPeriod(600);
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
            if (publicCloudRegion.contains(region)) {
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
