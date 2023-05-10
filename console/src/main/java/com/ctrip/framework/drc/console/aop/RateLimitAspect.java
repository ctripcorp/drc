package com.ctrip.framework.drc.console.aop;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.google.common.util.concurrent.RateLimiter;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
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

    @Around("pointcut()")
    public Object doBefore(ProceedingJoinPoint joinPoint) {
        if (!rateLimiter.tryAcquire()) {
            MethodSignature signature = (MethodSignature) joinPoint.getSignature();
            Method method = signature.getMethod();
            logger.warn("Service Current is Limiting, Method Name: {}", method.getName());
            return ApiResult.getFailInstance(null, "Service not available!");
        }
        return invokeMethod(joinPoint);
    }

    private Object invokeMethod(ProceedingJoinPoint joinPoint) {
        Object[] args = joinPoint.getArgs();
        try {
            return joinPoint.proceed(args);
        } catch (Throwable e) {
            logger.error("[[tag=AuthToken]] error", e);
            return null;
        }
    }
}
