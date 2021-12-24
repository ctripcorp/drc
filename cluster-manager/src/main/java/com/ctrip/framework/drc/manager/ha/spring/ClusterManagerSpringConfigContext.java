package com.ctrip.framework.drc.manager.ha.spring;

import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.config.DefaultClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerMultiDcServiceManager;
import com.ctrip.framework.drc.manager.ha.meta.server.impl.DefaultClusterManagerMultiDcServiceManager;
import com.ctrip.framework.drc.manager.ha.multidc.MultiDcNotifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @Author limingdong
 * @create 2020/4/20
 */
@Configuration
@ComponentScan(basePackages = {"com.ctrip.framework.drc.manager"})
public class ClusterManagerSpringConfigContext extends AbstractSpringConfigContext {

    @Bean
    public ClusterManagerConfig getMetaServerConfig(){
        return new DefaultClusterManagerConfig();
    }


    @Bean
    public ClusterManagerMultiDcServiceManager getMetaServerMultiDcServiceManager() {

        return new DefaultClusterManagerMultiDcServiceManager();
    }

    @Bean
    public MultiDcNotifier getMultiDcNotifier(){
        return new MultiDcNotifier();
    }

}

