package com.ctrip.framework.drc.service.console.web;

import com.ctrip.infosec.sso.client.logout.Logout;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class ServletConfig {

    @Bean
    public ServletRegistrationBean logoutServletRegistration() {

        ServletRegistrationBean registration = new ServletRegistrationBean();
        registration.setServlet(new Logout());
        Collection<String> urlMappings = new ArrayList<>();
        urlMappings.add("/logout");
        registration.setUrlMappings(urlMappings);
        registration.setLoadOnStartup(2);
        return registration;
    }
}
