package com.ctrip.framework.drc.console.aop;

import com.ctrip.framework.drc.console.dao.RowsFilterMetaTblDao;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaTbl;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
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

    @Pointcut("@annotation(com.ctrip.framework.drc.console.aop.AuthToken)")
    public void pointcut() {
    }

    @Before(value = "pointcut() && @annotation(authToken)")
    public void doBefore(JoinPoint joinPoint, AuthToken authToken) throws Exception {
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
            default:
                break;
        }

        checkToken(accessToken, metaFilterName);
    }

    private Map<String, Object> getParamMap(JoinPoint joinPoint) {
        Map<String, Object> paramMap = new HashMap<>();
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Object[] paramValues = joinPoint.getArgs();
        String[] paramNames = signature.getParameterNames();

        for (int i = 0; i < paramNames.length; i++) {
            paramMap.put(paramNames[i], paramValues[i]);
        }

        return paramMap;
    }

    private void checkToken(String accessToken, String metaFilterName) throws SQLException {
        if (StringUtils.isBlank(accessToken) || StringUtils.isBlank(metaFilterName)) {
            throw new IllegalArgumentException("Invalid AccessToken!");
        }

        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryOneByMetaFilterName(metaFilterName);
        if (rowsFilterMetaTbl == null || !accessToken.equals(rowsFilterMetaTbl.getToken())) {
            logger.error("Invalid AccessToken, metaFilterName: {}", metaFilterName);
            throw new IllegalArgumentException("Invalid AccessToken!");
        }
    }

}
