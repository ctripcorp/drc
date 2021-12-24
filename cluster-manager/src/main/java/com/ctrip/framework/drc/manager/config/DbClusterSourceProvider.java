package com.ctrip.framework.drc.manager.config;

import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.framework.drc.manager.service.ConsoleServiceImpl;
import com.ctrip.xpipe.config.AbstractConfigBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.META_LOGGER;

/**
 * Created by mingdongli
 * 2019/11/27 下午6:32.
 */
@Component
public class DbClusterSourceProvider extends AbstractConfigBean implements SourceProvider {

    @Autowired
    private ConsoleServiceImpl consoleService;

    public DbClusterSourceProvider() {
    }

    @Override
    public Dc getDc(String dcId) {
        String dbClusterString = consoleService.getLocalDbClusters();
        try {
            Drc drc = DefaultSaxParser.parse(dbClusterString);
            Dc dc = drc.getDcs().get(dcId);
            META_LOGGER.debug("[meta] dc {}", dc);
            return dc;
        } catch (Throwable t) {
            logger.error("[Parser] config({}) error, ", dbClusterString, t);
            return null;
        }
    }
}
