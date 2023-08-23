package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV3;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Set;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.META_LOGGER;

@Component
@Order(1)
public class DaoConfig extends AbstractConfig implements Config {

    @Autowired
    private MetaGeneratorV3 metaGeneratorV3;

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

            DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.meta.update", "dao", () -> {
                long s = System.currentTimeMillis();
                Drc drc = metaGeneratorV3.getDrc();
                META_LOGGER.info("update config in DaoConfig");
                long e = System.currentTimeMillis();
                META_LOGGER.info("dao update meta info, took {}ms", e - s);
                META_LOGGER.debug("[meta] dao generated drc: {}", drc);
                if (null != drc && drc.getDcs().size() != 0 && !drc.toString().equalsIgnoreCase(this.xml)) {
                    this.xml = drc.toString();
                    persistConfig();
                }
                long e2 = System.currentTimeMillis();
                META_LOGGER.info("dao update meta info, check and persist took {}ms", e2 - e);
            });
        } catch (Exception e) {
            META_LOGGER.error("Fail get drc from db, ", e);
        }
    }
}
