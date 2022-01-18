package com.ctrip.framework.drc.console;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;


/**
 * Created by mingdongli
 * 2019/10/30 上午10:59.
 */
@ServletComponentScan
@SpringBootApplication
@ComponentScan(basePackages = {"com.ctrip.framework.drc.console","com.ctrip.framework.drc.service.console"})
public class ConsoleApplication extends SpringBootServletInitializer {
    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(ConsoleApplication.class);
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder(ConsoleApplication.class).run(args);
    }
}
