package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.service.impl.MetaGenerator;
import com.ctrip.framework.drc.core.entity.Drc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Set;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.META_LOGGER;

@Component
@Order(1)
public class DaoConfig extends AbstractConfig implements Config{

    @Autowired
    private MetaGenerator metaGenerator;
    
    @Autowired
    private DataCenterService dataCenterService;

    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @Override
    public void updateConfig() {
        try {
            String region = consoleConfig.getRegion();
            Set<String> publicCloudRegion = consoleConfig.getPublicCloudRegion();
            if (publicCloudRegion.contains(region)) {
                return;
            }
            Drc drc = metaGenerator.getDrc();
            META_LOGGER.debug("[meta] generated drc: {}", drc);

            if (null != drc && drc.getDcs().size() != 0 && !drc.toString().equalsIgnoreCase(this.xml)) {
                this.xml = drc.toString();
                persistConfig();
            }
        } catch (Exception e) {
            META_LOGGER.error("Fail get drc from db, ", e);
        }
    }
    

}
