package com.ctrip.framework.drc.service.console;



import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;



@Configuration
public class FilterConfig {

    @Bean
    public FilterRegistrationBean ssoFilterRegistration() {
        FilterRegistrationBean registration = new FilterRegistrationBean();
        registration.setFilter(new CtripSSOFilterWithDegradeSwitch());
        registration.addUrlPatterns("/*");
        registration.setName("sessionFilter");
        return registration;
    }

}
