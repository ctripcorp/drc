package com.ctrip.framework.drc.service.console.web;



import com.ctrip.framework.drc.service.console.web.filter.CtripSSOFilterWithDegradeSwitch;
import com.ctrip.framework.drc.service.console.web.filter.IAMFilter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;



@Configuration
public class FilterRegisterConfig {

    @Bean
    public FilterRegistrationBean ssoFilterRegistration() {
        FilterRegistrationBean registration = new FilterRegistrationBean();
        registration.setFilter(new CtripSSOFilterWithDegradeSwitch());
        registration.addUrlPatterns("/*");
        registration.setName("sessionFilter");
        return registration;
    }

    @Bean
    public FilterRegistrationBean iamFilterRegistration() {
        FilterRegistrationBean registration = new FilterRegistrationBean();
        registration.setFilter(new IAMFilter());
        registration.addUrlPatterns("/*");
        registration.setName("IAMFilter");
        return registration;
    }

}
