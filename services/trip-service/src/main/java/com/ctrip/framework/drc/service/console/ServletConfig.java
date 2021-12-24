package com.ctrip.framework.drc.service.console;

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
//        for openSource use blankLogoutServlet
//        registration.setServlet(new BlankLogoutServlet());        
        registration.setServlet(new Logout());

        Map<String, String> initParamMap = new HashMap<>();
        // 登出后跳转到的url，各环境会不一样，最好通过qconfig配置。
        initParamMap.put("serverName", "http://localhost:8080");
        registration.setInitParameters(initParamMap);

        Collection<String> urlMappings = new ArrayList<>();
        urlMappings.add("/logout");
        registration.setUrlMappings(urlMappings);

        registration.setLoadOnStartup(2);

        return registration;
    }
}
