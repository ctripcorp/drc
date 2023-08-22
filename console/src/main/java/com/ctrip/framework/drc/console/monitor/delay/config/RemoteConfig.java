package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.core.http.HttpUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.META_LOGGER;

@Component
@Order(2)
public class RemoteConfig extends AbstractConfig implements Config {

    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Override
    public void updateConfig() {

        Set<String> localConfigCloudDc = consoleConfig.getLocalConfigCloudDc();
        String centerRegionUrl = consoleConfig.getCenterRegionUrl();
        if (localConfigCloudDc.contains(dbClusterSourceProvider.getLocalDcName())) {
            return;
        }
        if(!StringUtils.isEmpty(centerRegionUrl)) {
                try {
                    String drcFromRemote;
                    long s = System.currentTimeMillis();
                    if (DefaultConsoleConfig.SWITCH_ON.equals(consoleConfig.getMetaRealtimeSwitch())) {
                        drcFromRemote = HttpUtils.get(String.format("%s/api/drc/v2/meta/?refresh=true", centerRegionUrl), String.class);
                        META_LOGGER.info("remote update meta info with v2, refresh true, url: {}", drcFromRemote);
                    } else {
                        drcFromRemote = HttpUtils.get(String.format("%s/api/drc/v1/meta/", centerRegionUrl), String.class);
                        META_LOGGER.info("remote update meta info with v1, refresh false, url: {}", drcFromRemote);
                    }
                    long e = System.currentTimeMillis();
                    META_LOGGER.info("remote update meta info, took {}ms", e - s);
                    if(StringUtils.isNotBlank(drcFromRemote) && !drcFromRemote.equalsIgnoreCase(this.xml)) {
                        this.xml = drcFromRemote;
                        persistConfig();
                    }
                } catch (Throwable t) {
                    META_LOGGER.warn("Fail get drc from remote center region ", t);
                }
        }

    }
}
