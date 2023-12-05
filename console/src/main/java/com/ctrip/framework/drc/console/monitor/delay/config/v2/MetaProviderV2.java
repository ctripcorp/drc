package com.ctrip.framework.drc.console.monitor.delay.config.v2;

import com.ctrip.framework.drc.console.monitor.AbstractMonitor;
import com.ctrip.framework.drc.console.monitor.delay.config.CompositeConfig;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.PriorityOrdered;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName MetaProviderV2
 * @Author haodongPan
 * @Date 2023/7/10 14:53
 * @Version: $
 */
@Component
public class MetaProviderV2 extends AbstractMonitor implements PriorityOrdered {

    @Autowired
    private CompositeConfig compositeConfig;

    protected volatile String drcString;

    protected volatile Drc drc;

    public synchronized Drc getDrc() {
        if (drc == null) {
            scheduledTask();
        }
        return drc;
    }

    public Drc getRealtimeDrc() {
        scheduledTask();
        return drc;
    }

    public Drc getDrc(String dcId) {
        if (drc == null) {
            scheduledTask();
        }
        Dc dc = drc.findDc(dcId);
        Drc drcWithOneDc = new Drc();
        drcWithOneDc.addDc(dc);
        return drcWithOneDc;
    }

    public Drc getRealtimeDrc(String dcId) {
        scheduledTask();
        Dc dc = drc.findDc(dcId);
        Drc drcWithOneDc = new Drc();
        drcWithOneDc.addDc(dc);
        return drcWithOneDc;
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
        setInitialDelay(0);
        setPeriod(300);
        setTimeUnit(TimeUnit.SECONDS);
        super.initialize();
    }

    @Override // refresh when new config submit
    public void scheduledTask() {
        compositeConfig.updateConfig();
        String newDrcString = compositeConfig.getConfig();
        if (StringUtils.isNotBlank(newDrcString) && !newDrcString.equalsIgnoreCase(drcString)) {
            drcString = newDrcString;
            try {
                drc = DefaultSaxParser.parse(drcString);
            } catch (Throwable t) {
                logger.error("[Parser] config {} error", drcString);
            }
        }
    }

    @Override
    public int getOrder() {
        return 0;
    }

}
