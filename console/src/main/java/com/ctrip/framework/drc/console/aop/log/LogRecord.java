package com.ctrip.framework.drc.console.aop.log;

import com.ctrip.framework.drc.console.enums.operation.OperateAttrEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateTypeEnum;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface LogRecord {

    /**
     * @return execute success log template,{{SpEL}}
     */
    String success();

    /**
     * @return execute fail log template
     */
    String fail() default "";

    /**
     * @return persist to logDb
     */
    boolean persisted() default true;

    /**
     * @return operator, if empty get from sso
     */
    String operator() default "";

    /**
     * @return operate type
     */
    OperateTypeEnum type() ;
    
    /**
     * @return operate attribute
     */
    
    OperateAttrEnum attr();
    
}
