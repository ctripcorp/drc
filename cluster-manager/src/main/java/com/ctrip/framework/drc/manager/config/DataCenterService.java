package com.ctrip.framework.drc.manager.config;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.foundation.DefaultFoundationService;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

/**
 * Created by mingdongli
 * 2019/12/3 下午5:44.
 */
@Component
public class DataCenterService  extends AbstractConfigBean implements InitializingBean {

    public String LOCAL_DC;

    @Override
    public void afterPropertiesSet() {
        LOCAL_DC = FoundationService.DEFAULT.getDataCenter();
        System.setProperty(DefaultFoundationService.DATA_CENTER_KEY, LOCAL_DC);
        logger.info("[LOCAL_DC] is set {}", LOCAL_DC);
    }

    public String getDc() {
        return LOCAL_DC;
    }
}
