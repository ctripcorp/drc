package com.ctrip.framework.drc.console.aop;

import com.google.common.util.concurrent.RateLimiter;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

/**
 * Rows Filter Whitelist RateLimitAspect
 * Created by dengquanliang
 * 2023/5/5 16:51
 */
@Order(1)
@Aspect
@Component
public class RateLimitAspect {

    private static final Logger logger = LoggerFactory.getLogger(AuthTokenAspect.class);

    private final RateLimiter rateLimiter = RateLimiter.create(10.0);

    @Pointcut("@annotation(com.ctrip.framework.drc.console.aop.RateLimit)")
    public void pointcut() {
    }

    @Before("pointcut()")
    public void doBefore(JoinPoint joinPoint) {
        if (!rateLimiter.tryAcquire()) {
            MethodSignature signature = (MethodSignature) joinPoint.getSignature();
            Method method = signature.getMethod();
            logger.warn("Service Current is Limiting, Method Name: {}", method.getName());
            throw new RuntimeException("Unable to Provide Service!");
        }
    }


}
