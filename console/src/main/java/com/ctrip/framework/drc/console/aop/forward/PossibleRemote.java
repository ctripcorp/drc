package com.ctrip.framework.drc.console.aop.forward;

import com.ctrip.framework.drc.console.enums.ForwardTypeEnum;
import com.ctrip.framework.drc.console.enums.HttpRequestEnum;
import com.ctrip.framework.drc.core.http.ApiResult;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


// route HTTP GET via mha'dc ,use original arguments
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface PossibleRemote {
    
    String path();

    ForwardTypeEnum forwardType() default ForwardTypeEnum.TO_OVERSEA_BY_ARG;
    
    HttpRequestEnum httpType() default HttpRequestEnum.GET;
    
    String[] excludeArguments() default {};
    
    Class<? extends ApiResult>  responseType() default ApiResult.class;
}
