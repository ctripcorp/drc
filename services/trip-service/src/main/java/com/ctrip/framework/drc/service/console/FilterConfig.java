package com.ctrip.framework.drc.service.console;


import com.ctrip.infosec.sso.client.CtripSSOFilter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.servlet.DispatcherType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


@Configuration
public class FilterConfig {

    @Bean
    public FilterRegistrationBean ssoFilterRegistration() {

        FilterRegistrationBean registration = new FilterRegistrationBean();
        registration.setFilter(new CtripSSOFilter());
        registration.addUrlPatterns("/*");
        registration.setName("sessionFilter");
        return registration;
    }

}
