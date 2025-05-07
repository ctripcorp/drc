package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.service.security.HeraldService;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.META_LOGGER;

@Component
@Order(2)
public class RemoteConfig extends AbstractConfig implements Config {

    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private DataCenterService dataCenterService;

    private HeraldService heraldService = ApiContainer.getHeraldServiceImpl();

    @Override
    public void updateConfig() {
        String centerRegionUrl = consoleConfig.getCenterRegionUrl();
        if (!StringUtils.isEmpty(centerRegionUrl)) {
            try {
                DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.meta.update", "remote", () -> {
                    long s = System.currentTimeMillis();
                    String requestUrl = String.format("%s/api/drc/v2/meta/?refresh=true", centerRegionUrl);
                    if (consoleConfig.requestWithHeraldToken()) {
                        requestUrl += "&heraldToken=" + heraldService.getLocalHeraldToken();
                    }
                    String drcFromRemote = HttpUtils.getAcceptAllEncoding(requestUrl, String.class);
                    META_LOGGER.info("remote update meta info with v2, refresh true");
                    long e = System.currentTimeMillis();
                    META_LOGGER.info("remote update meta info, took {}ms", e - s);
                    if (META_LOGGER.isDebugEnabled()) {
                        META_LOGGER.debug("[meta] remote generated drc: {}", drcFromRemote);
                    }
                    if (StringUtils.isNotBlank(drcFromRemote) && !drcFromRemote.equalsIgnoreCase(this.xml)) {
                        this.xml = drcFromRemote;
                        this.drc = DefaultSaxParser.parse(this.xml);
                        persistConfig();
                    }
                    long e2 = System.currentTimeMillis();
                    META_LOGGER.info("remote update meta info, check and persist took {}ms", e2 - e);
                });
            } catch (Throwable t) {
                META_LOGGER.warn("Fail get drc from remote center region ", t);
            }
        }
    }
}
