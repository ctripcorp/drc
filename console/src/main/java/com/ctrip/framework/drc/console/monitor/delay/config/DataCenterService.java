package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.config.AbstractConfigBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-12
 * copy from CM, may need to move to core later
 */
@Component
public class DataCenterService  extends AbstractConfigBean implements InitializingBean {

    public String LOCAL_DC;

    @Override
    public void afterPropertiesSet() {
        LOCAL_DC = FoundationService.DEFAULT.getDataCenter();
        logger.info("[LOCAL_DC] is set {}", LOCAL_DC);
    }

    public String getDc() {
        return LOCAL_DC;
    }
}
