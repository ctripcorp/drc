package com.ctrip.framework.drc.core.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-06-19
 */
@Component("springUtils")
public class SpringUtils implements ApplicationContextAware {

    private static ApplicationContext applicationContext = null;

    @Override
    public void setApplicationContext(ApplicationContext context) throws BeansException {
        if(null == applicationContext) {
            applicationContext = context;
        }
    }

    public static ApplicationContext getApplicationContext(){
        return applicationContext;
    }
}
