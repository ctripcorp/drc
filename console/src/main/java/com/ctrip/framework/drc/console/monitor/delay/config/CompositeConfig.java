package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.META_LOGGER;

@Component
public class CompositeConfig extends AbstractConfig implements Config {

    @Autowired
    private List<Config> configs;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Override
    public void updateConfig() {
        if (monitorTableSourceProvider.getDrcMetaXmlUpdateSwitch().equals(SWITCH_STATUS_ON)) {
            for (Config config : configs) {
                config.updateConfig();
                String c = config.getConfig();
                if(null != c) {
                    META_LOGGER.info("[META] from {}", config.getSourceType());
                    META_LOGGER.debug("[META] from {}: {}", config.getSourceType(), c);
                    xml = c;
                    break;
                }
            }
        } else {
            META_LOGGER.info("[META] metadb data update ,not update");
        }
    }

    @VisibleForTesting
    protected void addConfig(Config config) {
        initConfigs();
        configs.add(config);
    }

    @VisibleForTesting
    protected void initConfigs() {
        if (configs == null) {
            configs = Lists.newArrayList();
        }
    }
}
