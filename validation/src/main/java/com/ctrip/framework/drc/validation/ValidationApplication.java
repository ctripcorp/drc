package com.ctrip.framework.drc.validation;

import com.ctrip.framework.drc.core.server.container.ComponentRegistryHolder;
import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.lifecycle.CreatedComponentRedistry;
import com.ctrip.xpipe.lifecycle.DefaultRegistry;
import com.ctrip.xpipe.lifecycle.SpringComponentRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author shenhaibo
 * @version 1.0
 * 2021/3/11
 */
@SpringBootApplication
public class ValidationApplication {
    private static Logger logger = LoggerFactory.getLogger(ValidationApplication.class);

    public static void main(String[] args) throws Exception {
//        System.setProperty("ucs.dev.mode", Boolean.TRUE.toString());
        SpringApplication application = new SpringApplication(ValidationApplication.class);
        application.setRegisterShutdownHook(false);
        final ConfigurableApplicationContext context = application.run(args);
        final ComponentRegistry registry = initComponentRegistry(context);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
//                try {
//                    logger.info("[run][shutdown][release]");
//                    ApplierServerContainer abstractResourceManager = registry.getComponent(ApplierServerContainer.class);
//                    if (abstractResourceManager != null) {
//                        abstractResourceManager.release();
//                    }
//                } catch (Exception e) {
//                    logger.error("[run][shutdown][release]", e);
//                }
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
