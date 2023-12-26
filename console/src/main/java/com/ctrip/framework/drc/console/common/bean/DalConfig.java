package com.ctrip.framework.drc.console.common.bean;

import com.ctrip.platform.dal.dao.DalClient;
import com.ctrip.platform.dal.dao.DalClientFactory;
import com.ctrip.platform.dal.dao.DalQueryDao;
import com.ctrip.platform.dal.dao.helper.DalClientFactoryListener;
import org.springframework.boot.web.servlet.ServletListenerRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author wangjixin
 * @version 1.0
 * date: 2020-02-25
 */
@Configuration
public class DalConfig {

    public static final String DRC_TITAN_KEY = "fxdrcmetadb_w";

    @Bean
    public DalQueryDao dalQueryDao() {
        return new DalQueryDao(DRC_TITAN_KEY);
    }

    @Bean
    public DalClient dalClient() {
        return DalClientFactory.getClient(DRC_TITAN_KEY);
    }

    @Bean
    public ServletListenerRegistrationBean dalListener() {
        return new ServletListenerRegistrationBean<>(new DalClientFactoryListener());
    }
}
