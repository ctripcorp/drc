package com.ctrip.framework.drc.console.aop.permission;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.enums.TokenType;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.core.http.ApiResult;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.DigestUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * @ClassName AccessTokenAspect
 * @Author haodongPan
 * @Date 2023/11/30 16:13
 * @Version: $
 */
@Order(2)
@Aspect
@Component
public class AccessTokenAspect {

    private static final Logger logger = LoggerFactory.getLogger(AccessTokenAspect.class);

    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @Pointcut("within(com.ctrip.framework.drc.console.controller..*) && " +
            "@annotation(com.ctrip.framework.drc.console.aop.permission.AccessToken)")
    public void pointcut() {}

    @Around(value = "pointcut() && @annotation(accessToken)")
    public Object around(ProceedingJoinPoint joinPoint, AccessToken accessToken) throws Exception {
        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
        String token = request.getHeader("DRC-Access-Token");
        TokenType type = accessToken.type();
        Pair<Boolean, String> checkTokenResult = checkToken(token, type, accessToken);
        if (!checkTokenResult.getLeft()) {
            return ApiResult.getFailInstance(null,checkTokenResult.getRight());
        } else {
            return invokeMethod(joinPoint);
        }


    }
    
    private Pair<Boolean,String> checkToken(String token, TokenType type, AccessToken accessToken) throws Exception {
        if (StringUtils.isBlank(token)) {
            return Pair.of(false, "DRC-Access-Token is blank!");
        }
        String tokenKey = consoleConfig.getDrcAccessTokenKey();
        StringBuilder unEncodedToken  = new StringBuilder(tokenKey);
        if (accessToken.envSensitive()) {
            unEncodedToken.append("_").append(EnvUtils.getEnvStr());
        }
        unEncodedToken.append("_").append(type.getCode());
        String encodedToken = DigestUtils.md5DigestAsHex(unEncodedToken.toString().getBytes());
        if (!encodedToken.equals(token)) {
            return Pair.of(false, "DRC-Access-Token is invalid!");
        }
        return Pair.of(true, null);
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
