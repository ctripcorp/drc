package com.ctrip.framework.drc.performance;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.boot.web.support.SpringBootServletInitializer;

/**
 * Created by jixinwang on 2021/8/16
 */
@ServletComponentScan
@SpringBootApplication
public class ApplierTestApplication extends SpringBootServletInitializer {

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(ApplierTestApplication.class);
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder(ApplierTestApplication.class).run(args);
    }
}
