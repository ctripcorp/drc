package com.ctrip.framework.drc.console.aop;

import com.ctrip.framework.drc.console.enums.HttpRequestParamEnum;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by dengquanliang
 * 2023/5/5 11:41
 * ! only for rowsMetaFilter, api token: AccessToken
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface AuthToken {

    String name();
    HttpRequestParamEnum type();
}
