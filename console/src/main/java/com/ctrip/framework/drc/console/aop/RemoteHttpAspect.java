package com.ctrip.framework.drc.console.aop;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ForwardTypeEnum;
import com.ctrip.framework.drc.console.enums.HttpRequestEnum;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
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
import java.util.*;

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
    
    private  DalUtils dalUtils = DalUtils.getInstance();
    
    @Pointcut("@annotation(com.ctrip.framework.drc.console.aop.PossibleRemote)")
    public void pointCut(){};
    
    @Around(value = "pointCut() && @annotation(possibleRemote)")
    public Object aroundOperate(ProceedingJoinPoint point,PossibleRemote possibleRemote) {
        try {
            String localRegion = consoleConfig.getRegion();
            Set<String> publicCloudRegion = consoleConfig.getPublicCloudRegion();
            Map<String, String> consoleRegionUrls = consoleConfig.getConsoleRegionUrls();
            
            Map<String, Object> argsNotExcluded = getArgsNotExcluded(point,possibleRemote.excludeArguments());
            ForwardTypeEnum forwardType = possibleRemote.forwardType();
            
            switch (forwardType) {
                case TO_META_DB: 
                    if (publicCloudRegion.contains(localRegion)) {
                        StringBuilder url = new StringBuilder(consoleConfig.getCenterRegionUrl());
                        return forwardByHttp(url,possibleRemote,argsNotExcluded);
                    } else {
                        return invokeOriginalMethod(point);
                    }
                case TO_OVERSEA_BY_ARG:
                    if (!publicCloudRegion.contains(localRegion) ) {
                        String regionByArgs = getRegionByArgs(argsNotExcluded);
                        if (publicCloudRegion.contains(regionByArgs)) {
                            StringBuilder url = new StringBuilder(consoleRegionUrls.get(regionByArgs));
                            return forwardByHttp(url,possibleRemote,argsNotExcluded);
                        } else {
                            return invokeOriginalMethod(point);
                        }
                    } else {
                        return invokeOriginalMethod(point);
                    }
                default:
                    return invokeOriginalMethod(point);
            }
        } catch (Throwable e) {
            logger.error("[[tag=remoteHttpAop]] error", e);
            return null;
        }
    }
    
    private Object invokeOriginalMethod(ProceedingJoinPoint point) throws Throwable{
        Object[] args = point.getArgs();
        return point.proceed(args);
    }
    
    private Object forwardByHttp(StringBuilder regionUrl, PossibleRemote possibleRemote, Map<String, Object> args) {
        if (StringUtils.isEmpty(regionUrl)) {
            throw new IllegalArgumentException("no regionUrl find");
        }
        
        regionUrl.append(possibleRemote.path());
        boolean isFirstArgs = true;
        for (Map.Entry<String, Object> entry : args.entrySet()) {
            if (isFirstArgs) {
                regionUrl.append("?").append(entry.getKey()).append("=").append(entry.getValue());
                isFirstArgs = false;
            } else {
                regionUrl.append("&").append(entry.getKey()).append("=").append(entry.getValue());
            }
        }
        logger.info("[[tag=remoteHttpAop]] remote invoke console via Http url:{}", regionUrl);
        
        ApiResult apiResult;
        HttpRequestEnum httpRequestType = possibleRemote.httpType();
        switch (httpRequestType) {
            case GET:
                apiResult = HttpUtils.get(regionUrl.toString(), possibleRemote.responseType());
                break;
            case PUT:
                apiResult = HttpUtils.put(regionUrl.toString(), possibleRemote.responseType());
                break;
            case POST:
                apiResult = HttpUtils.post(regionUrl.toString(), possibleRemote.responseType());
                break;
            case DELETE:
                apiResult = HttpUtils.delete(regionUrl.toString(), possibleRemote.responseType());
                break;
            default:
                logger.error("[[tag=remoteHttpAop]] unsupported HttpRequestMethod" + httpRequestType.getDescription());
                return null;
        }
        return apiResult.getData();
    }

    private String getDcNameByArgs(Map<String, Object> arguments) {
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
    
    private String getRegionByArgs(Map<String, Object> arguments) {
        String dcNameByArgs = getDcNameByArgs(arguments);
        if (StringUtils.isEmpty(dcNameByArgs)) {
            return null;
        }  else {
            return consoleConfig.getRegionForDc(dcNameByArgs);
        }
    }
    
    /**
     * 获取参数Map集合
     * @param joinPoint
     * @return
     */
    private Map<String, Object> getArgsNotExcluded(ProceedingJoinPoint joinPoint,String[] excludeArguments) throws NoSuchMethodException{
        HashSet<String> excludeArgs = Sets.newHashSet(excludeArguments);
        Map<String, Object> param = new HashMap<>();
        MethodSignature signature = (MethodSignature)joinPoint.getSignature();
        Method method = signature.getMethod();
        Object[] paramValues = joinPoint.getArgs();
        String[] paramNames = signature.getParameterNames();
        for (int i = 0; i < paramNames.length; i++) {
            if (excludeArgs.contains(paramNames[i])) {
                continue;
            }
            logger.debug("[[tag=remoteHttpAop]] method:{},argName:{},argValue:{}",method.getName(),paramNames[i],paramValues[i]);
            param.put(paramNames[i], paramValues[i]);
        }
        return param;
    }
    
}
    
