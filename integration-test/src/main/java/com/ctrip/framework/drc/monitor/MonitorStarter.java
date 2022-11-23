package com.ctrip.framework.drc.monitor;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.boot.web.support.SpringBootServletInitializer;

/**
 * Created by mingdongli
 * 2019/11/1 下午4:56.
 */
@ServletComponentScan
@SpringBootApplication
public class MonitorStarter extends SpringBootServletInitializer {

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(MonitorStarter.class);
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder(MonitorStarter.class).run(args);
    }
}
