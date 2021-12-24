package com.ctrip.framework.drc.manager;

import com.ctrip.framework.drc.core.server.container.ComponentRegistryHolder;
import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.lifecycle.CreatedComponentRedistry;
import com.ctrip.xpipe.lifecycle.DefaultRegistry;
import com.ctrip.xpipe.lifecycle.SpringComponentRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Created by mingdongli
 * 2019/11/2 下午4:36.
 */
@SpringBootApplication
public class ClusterManagerApplication extends SpringBootServletInitializer {

    private static Logger logger = LoggerFactory.getLogger(ClusterManagerApplication.class);

    public static void main(String[] args) throws Exception {

        SpringApplication application = new SpringApplication(ClusterManagerApplication.class);
        application.setRegisterShutdownHook(false);
        final ConfigurableApplicationContext context = application.run(args);

        final ComponentRegistry registry = initComponentRegistry(context);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {

                try {
                    logger.info("[run][shutdown][stop]");
                    registry.stop();
                } catch (Exception e) {
                    logger.error("[run][shutdown][stop]", e);
                }
                try {
                    logger.info("[run][shutdown][dispose]");
                    registry.dispose();
                } catch (Exception e) {
                    logger.error("[run][shutdown][dispose]", e);
                }

                try {
                    logger.info("[run][shutdown][destroy]");
                    registry.destroy();
                } catch (Exception e) {
                    logger.error("[run][shutdown][destroy]", e);
                }
            }
        }));

    }

    private static ComponentRegistry initComponentRegistry(ConfigurableApplicationContext context) throws Exception {

        final ComponentRegistry registry = new DefaultRegistry(new CreatedComponentRedistry(),
                new SpringComponentRegistry(context));
        registry.initialize();
        registry.start();
        ComponentRegistryHolder.initializeRegistry(registry);
        return registry;
    }
}
