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
//         for openSource Filter use Blank
//         registration.setFilter(new BlankFilter());
        registration.setFilter(new CtripSSOFilter());
        registration.setDispatcherTypes(DispatcherType.REQUEST);

        // 这里用 qconfig 来获取 serverName
        //MapConfig config = MapConfig.get("app.properties");
        //Map<String, String> map = config.asMap();
        //String serverName = map.get("cas.sso.server.name");
        String serverName = "http://localhost:8080";
        Map<String, String> initParamMap = new HashMap<>();
        initParamMap.put("exclude_paths", "/api/drc/v1/switch/clusters/, /api/drc/v1/data/types, /api/drc/v1/gtid/fill, /api/drc/v1/logs, /api/drc/v1/logs/record, /api/drc/v1/meta, /api/drc/v1/beacon/, /api/drc/v1/mhas, /api/drc/v1/monitor, /api/drc/v1/monitor/consistency/history, /api/drc/v1/unit, /api/drc/v1/access");
        initParamMap.put("isCluster", "true");

        // 这里需要填入自己的站点url，例如本地调试时为"http://localhost:8080"，各环境会不一样，最好通过qconfig配置。
        initParamMap.put("serverName", serverName);
        
        initParamMap.put("encoding", "UTF-8");
        registration.setInitParameters(initParamMap);

        Collection<String> urlPatterns = new ArrayList<>();
        urlPatterns.add("/*");
        registration.setUrlPatterns(urlPatterns);

        registration.setOrder(1);

        return registration;
    }

}
