package com.ctrip.framework.drc.console.aop;

import com.ctrip.framework.drc.console.dao.RowsFilterMetaTblDao;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaTbl;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Rows Filter Whitelist AuthTokenAspect
 * Created by dengquanliang
 * 2023/5/5 11:45
 */
@Order(2)
@Aspect
@Component
public class AuthTokenAspect {

    private static final Logger logger = LoggerFactory.getLogger(AuthTokenAspect.class);

    @Autowired
    private RowsFilterMetaTblDao rowsFilterMetaTblDao;

    @Pointcut("within(com.ctrip.framework.drc.console.controller..*) && " +
            "@annotation(com.ctrip.framework.drc.console.aop.AuthToken)")
    public void pointcut() {
    }

    @Around(value = "pointcut() && @annotation(authToken)")
    public Object around(ProceedingJoinPoint joinPoint, AuthToken authToken) throws Exception {
        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
        String accessToken = request.getHeader("Access-Token");

        String metaFilterName = "";
        Map<String, Object> paramMap = getParamMap(joinPoint);
        String paramName = authToken.name();

        switch (authToken.type()) {
            case PATH_VARIABLE:
                metaFilterName = String.valueOf(paramMap.get(paramName));
                break;
            case REQUEST_BODY:
                String paramValue = JsonUtils.toJson(paramMap.get("param"));
                JsonObject jsonObject = JsonUtils.fromJson(paramValue, JsonObject.class);
                if (jsonObject.get(paramName) != null) {
                    metaFilterName = jsonObject.get(paramName).getAsString();
                }
                break;
            default:
                break;
        }

        return checkToken(accessToken, metaFilterName, joinPoint);
    }

    private Map<String, Object> getParamMap(ProceedingJoinPoint joinPoint) {
        Map<String, Object> paramMap = new HashMap<>();
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Object[] paramValues = joinPoint.getArgs();
        String[] paramNames = signature.getParameterNames();

        for (int i = 0; i < paramNames.length; i++) {
            paramMap.put(paramNames[i], paramValues[i]);
        }

        return paramMap;
    }

    private Object checkToken(String accessToken, String metaFilterName, ProceedingJoinPoint joinPoint) throws Exception {
        if (StringUtils.isBlank(metaFilterName)) {
            return ApiResult.getFailInstance("MetaFilterName Requires Not Empty!");
        }
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryOneByMetaFilterName(metaFilterName);
        if (rowsFilterMetaTbl == null) {
            return ApiResult.getFailInstance("MetaFilterName Not Exist!");
        }
        if (StringUtils.isBlank(accessToken)) {
            return ApiResult.getFailInstance("Require AccessToken!");
        }
        if (!accessToken.equals(rowsFilterMetaTbl.getToken())) {
            return ApiResult.getFailInstance("Invalid AccessToken!");
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
