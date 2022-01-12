package com.ctrip.framework.drc.service.console;


import com.ctrip.infosec.sso.client.CtripSSOFilter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;



@Configuration
public class FilterConfig {

    @Bean
    public FilterRegistrationBean ssoFilterRegistration() {
        // config at bypass/filter/permission on qconfig permissionsconfig.xml
        FilterRegistrationBean registration = new FilterRegistrationBean();
        registration.setFilter(new CtripSSOFilter());
        registration.addUrlPatterns("/*");
        registration.setName("sessionFilter");
        return registration;
    }

}
