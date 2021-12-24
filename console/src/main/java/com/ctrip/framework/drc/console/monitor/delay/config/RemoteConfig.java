package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.core.http.HttpUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Map;

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
        Map<String, String> consoleDcInfos = consoleConfig.getConsoleDcInfos();
        if(consoleDcInfos.size() != 0) {
            for(Map.Entry<String, String> entry : consoleDcInfos.entrySet()) {
                if(!entry.getKey().equalsIgnoreCase(dbClusterSourceProvider.getLocalDcName())) {
                    try {
                        String drcFromRemote = HttpUtils.get(String.format("%s/api/drc/v1/meta/", entry.getValue()), String.class);
                        if(StringUtils.isNotBlank(drcFromRemote) && !drcFromRemote.equalsIgnoreCase(this.xml)) {
                            this.xml = drcFromRemote;
                            persistConfig();
                            break;
                        }
                    } catch (Throwable t) {
                        META_LOGGER.warn("Fail get drc from remote: {}", entry.getKey(), t);
                    }
                }
            }
        }

    }
}
