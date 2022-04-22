package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.SSOService;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.utils.SpringUtils;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.ops.AppNode;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.stereotype.Service;

import javax.servlet.Filter;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * @ClassName NotifyServiceImpl
 * @Author haodongPan
 * @Date 2022/3/17 21:24
 * @Version: $
 */
@Service
public class SSOServiceImpl implements SSOService {
    
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String degradeUrl = "http://%s:%s/api/drc/v1/access/sso/degrade/notify/%s";
    private final String localAppId = "100023928";
    
    @Autowired
    private DomainConfig domainConfig;
    
    OPSApiService opsApiService = ApiContainer.getOPSApiServiceImpl();
    
    
    @Override
    public ApiResult degradeAllServer(Boolean isOpen) {
        ApiResult apiResult = setDegradeSwitch(isOpen);
        if (apiResult.getStatus().equals(ResultCode.HANDLE_FAIL.getCode())) {
            return apiResult;
        } else {
            try {
                return notifyOtherMachine(isOpen);
            } catch (UnknownHostException e) {
                logger.warn("notify other machine fail", e);
                return ApiResult.getFailInstance("notify other machine fail");
            }
        }
    }
        
    @Override
    public ApiResult setDegradeSwitch(Boolean isOpen){
        logger.info("[[switch=ssoDegrade]] changeSSOFilterStatus to {}",isOpen);
        try {
            FilterRegistrationBean ssoFilterRegistration = (FilterRegistrationBean) SpringUtils.getApplicationContext().
                    getBean("ssoFilterRegistration");
            Filter ssoFilter = ssoFilterRegistration.getFilter();
            Class<? extends Filter> ssoFilterClass = ssoFilter.getClass();
            Method setSSODegradeMethod = ssoFilterClass.getMethod("setSSODegrade", Boolean.class);
            Boolean invokeResult = (Boolean) setSSODegradeMethod.invoke(ssoFilter, isOpen);
            if (isOpen.equals(invokeResult)) {
                return ApiResult.getSuccessInstance("set ssoDegradeSwitch to :" + isOpen);
            } else {
                return ApiResult.getFailInstance("invoke result not equal the expected");
            }
        } catch (Exception e) {
            logger.error("[[switch=ssoDegrade]] changeSSOFilterStatus occur error",e);
            return ApiResult.getFailInstance(e);
        }
    }
    
    @Override
    public ApiResult notifyOtherMachine(Boolean isOpen) throws UnknownHostException {
        List<AppNode> appNodes = getAppNodes();
        InetAddress localHost = InetAddress.getLocalHost();
        for (AppNode appNode : appNodes) {
            if (appNode.getIp().equals(localHost.getHostAddress()) || !appNode.isLegal()) {
                continue;
            }
            String url = String.format(degradeUrl, appNode.getIp(), appNode.getPort(), isOpen);
            try {
                ApiResult postResult = HttpUtils.post(url, null, ApiResult.class);
                if (postResult.getStatus().equals(ResultCode.HANDLE_FAIL.getCode())) {
                    logger.warn("notify other machine fail,ip:port is {}:{}", appNode.getIp(), appNode.getPort());
                }
            } catch (Exception e) {
               logger.error("notify other machine fail,ip:port is {}:{}", appNode.getIp(), appNode.getPort(),e);
               continue;
            }
            logger.info("notify other machine success,ip:port is {}:{}", appNode.getIp(), appNode.getPort());
        }
        return ApiResult.getSuccessInstance("notifyAllMachineDegradeSwitch openStatus to " + isOpen );
    }
    
    @Override
    public List<AppNode> getAppNodes(){
        String cmsGetServerUrl = domainConfig.getCmsGetServerUrl();
        String opsAccessToken = domainConfig.getOpsAccessToken();
        String env = cmsGetServerUrl.contains("FAT") ? "FAT" : null;
        return opsApiService.getAppNodes(cmsGetServerUrl, opsAccessToken, Lists.newArrayList(localAppId), env);
    }
    
    
}
