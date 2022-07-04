package com.ctrip.framework.drc.console.aop;

import com.ctrip.framework.drc.console.enums.HttpRequestEnum;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


// route HTTP GET via mha'dc ,use original arguments
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface PossibleRemote {
    String path();
    HttpRequestEnum httpType() default HttpRequestEnum.GET;
}
