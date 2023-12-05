package com.ctrip.framework.drc.console.aop.permission;

import com.ctrip.framework.drc.console.enums.TokenType;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface AccessToken {
    TokenType type();
    
    boolean envSensitive() default false;
}
