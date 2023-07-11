package com.ctrip.framework.drc.console.monitor.delay.config.v2;

import com.ctrip.framework.drc.console.config.ConsoleConfig;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.AbstractMonitor;
import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV2;
import com.ctrip.framework.drc.core.entity.Drc;
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
    
    public Drc getDrc() {
        if (drc == null) {
           scheduledTask(); 
        }
        return drc;
    }
    

    @Override
    public void initialize() {
        setInitialDelay(30);
        setPeriod(180);
        setTimeUnit(TimeUnit.SECONDS);
        super.initialize();
    }
    
    @Override
    public synchronized void scheduledTask() {
        try {
            String region = consoleConfig.getRegion();
            Set<String> publicCloudRegion = consoleConfig.getPublicCloudRegion();
            if (publicCloudRegion.contains(region)) {
                return;
            }
            drc = metaGeneratorV2.getDrc();
        } catch (Throwable t) {
            logger.error("[[meta=v2]] MetaProviderV2 get drc fail", t);
        }
        
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
