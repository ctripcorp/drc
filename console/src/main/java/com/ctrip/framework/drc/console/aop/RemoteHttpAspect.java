package com.ctrip.framework.drc.console.aop;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.google.common.collect.Sets;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @ClassName RemoteAspect
 * @Author haodongPan
 * @Date 2022/6/21 16:57
 * @Version: $
 */
@Component
@Aspect
public class RemoteHttpAspect {

    private static final Logger logger = LoggerFactory.getLogger(RemoteHttpAspect.class);
    
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    
    private final DalUtils dalUtils = DalUtils.getInstance();
    
    @Pointcut("@annotation(com.ctrip.framework.drc.console.aop.PossibleRemote)")
    public void pointCut(){};
    
    @Around(value = "pointCut()")
    public Object aroundOperate(ProceedingJoinPoint point) {
        try {
            Map<String, Object> nameAndValue = getArguments(point);
            String dcName = getDcNameByArgument(nameAndValue);
            if (dcName != null) {
                Map<String, String> consoleDcInfos = consoleConfig.getConsoleDcInfos();
                Set<String> publicCloudDc = consoleConfig.getPublicCloudDc();
                if (publicCloudDc.contains(dcName)) {
                    PossibleRemote annotation = getAnnotation(point);
                    StringBuilder url = new StringBuilder(consoleDcInfos.get(dcName));
                    url.append(annotation.path());
                    boolean isFirstArgs = true;
                    for (Map.Entry<String, Object> entry : nameAndValue.entrySet()) {
                        if (isFirstArgs) {
                            url.append("?").append(entry.getKey()).append("=").append(entry.getValue());
                            isFirstArgs = false;
                        } else {
                            url.append("&").append(entry.getKey()).append("=").append(entry.getValue());
                        }
                    }
                    logger.info("[[tag=remoteHttpAop]] remote invoke console via Http url:{}", url);
                    ApiResult apiResult;
                    switch (annotation.httpType()) {
                        case GET: 
                            apiResult = HttpUtils.get(url.toString(), ApiResult.class);
                            break;
                        case PUT: 
                            apiResult = HttpUtils.put(url.toString(), ApiResult.class);
                            break;
                        case POST: 
                            apiResult = HttpUtils.post(url.toString(), ApiResult.class);
                            break;
                        case DELETE: 
                            apiResult = HttpUtils.delete(url.toString(), ApiResult.class);
                            break;
                        default:
                            logger.error("[[tag=remoteHttpAop]] unsupported HttpRequestMethod" + annotation.httpType().getDescription());
                            return null;
                    }
                    return apiResult.getData();
                } else {
                    Object[] args = point.getArgs();
                    return point.proceed(args);
                }
            } else {
                Object[] args = point.getArgs();
                return point.proceed(args);
            }
        } catch (Throwable e) {
            logger.error("[[tag=remoteHttpAop]] error", e);
            return null;
        }
    }

    private String getDcNameByArgument(Map<String, Object> arguments) {
        try {
            String dcName = null;
            if (arguments.containsKey("mha") || arguments.containsKey("mhaName") ||
                    arguments.containsKey("dc") || arguments.containsKey("dcName")) {
                if (arguments.containsKey("dc")) {
                    dcName = (String) arguments.get("dc");
                } else if (arguments.containsKey("dcName"))  {
                    dcName = (String) arguments.get("dcName");
                } else if (arguments.containsKey("mha")) {
                    dcName = dalUtils.getDcName((String) arguments.get("mha"), BooleanEnum.FALSE.getCode());
                } else if (arguments.containsKey("mhaName")) {
                    dcName = dalUtils.getDcName((String) arguments.get("mhaName"), BooleanEnum.FALSE.getCode());
                }
            }
            return dcName;
        } catch (SQLException e) {
            logger.error("[[tag=remoteHttpAop]] sql error", e);
            return null;
        }
    }
    
    /**
     * 获取参数Map集合
     * @param joinPoint
     * @return
     */
    private Map<String, Object> getArguments(ProceedingJoinPoint joinPoint) throws NoSuchMethodException{
        PossibleRemote annotation = getAnnotation(joinPoint);
        HashSet<String> excludeArgs = Sets.newHashSet(annotation.excludeArguments());
        Map<String, Object> param = new HashMap<>();
        Object[] paramValues = joinPoint.getArgs();
        String[] paramNames = ((MethodSignature)joinPoint.getSignature()).getParameterNames();
        for (int i = 0; i < paramNames.length; i++) {
            if (excludeArgs.contains(paramNames[i])) {
                continue;
            }
            param.put(paramNames[i], paramValues[i]);
        }
        return param;
    }
    
    private PossibleRemote getAnnotation(ProceedingJoinPoint point) throws NoSuchMethodException {
        MethodSignature signature = (MethodSignature)point.getSignature();
        Method method = signature.getMethod();
        PossibleRemote annotation = point.getTarget().getClass().
                getDeclaredMethod(method.getName(),method.getParameterTypes()).getAnnotation(PossibleRemote.class);
        return annotation;
    }

}
    
