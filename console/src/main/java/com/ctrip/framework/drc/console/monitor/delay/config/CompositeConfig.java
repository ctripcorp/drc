package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.META_LOGGER;

@Component
public class CompositeConfig extends AbstractConfig implements Config {

    @Autowired
    private List<Config> configs;

    @Override
    public void updateConfig() {
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
