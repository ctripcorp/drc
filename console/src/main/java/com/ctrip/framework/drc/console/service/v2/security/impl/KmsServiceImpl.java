package com.ctrip.framework.drc.console.service.v2.security.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.param.v2.security.Account;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.v2.security.KmsService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.security.HeraldService;
import com.ctrip.framework.foundation.Foundation;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @ClassName KmsServiceImpl
 * @Author haodongPan
 * @Date 2024/3/25 16:23
 * @Version: $
 */
@Service
public class KmsServiceImpl implements KmsService {
    
    private final Logger logger = LoggerFactory.getLogger(KmsServiceImpl.class);
    
    private Map<String,String> tokenSecretKeyCache = new ConcurrentHashMap<>();
    private Map<String,Account> tokenAccountCache = new ConcurrentHashMap<>();
    
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    
    private HeraldService heraldService = ApiContainer.getHeraldServiceImpl();
    

    @Override
    //http://conf.ctripcorp.com/pages/viewpage.action?pageId=1732205587#id-07KMS%E6%8E%A5%E5%85%A5&QA%E6%96%87%E6%A1%A3-%E4%B8%83%E3%80%81%E6%8E%A5%E5%8F%A3%E8%B0%83%E7%94%A8
    public String getSecretKey(String accessToken) { 
        // get keyValue from kms via https request, should be loaded before generate metaInfo
        if (tokenSecretKeyCache.containsKey(accessToken)) {
            return tokenSecretKeyCache.get(accessToken);
        }
        String envStr = EnvUtils.getEnvStr();
        String kmsUrl = consoleConfig.getKmsUrl(envStr);
        Map<String, Object> paramsMap = Maps.newHashMap();
        paramsMap.put("token", accessToken);
        paramsMap.put("appid", Foundation.app().getAppId());
        paramsMap.put("herald-token", heraldService.getLocalHeraldToken());
        String queryParam= "/query-key?token={token}&appid={appid}&herald-token={herald-token}";
        String responseBody = HttpUtils.get(kmsUrl + queryParam, String.class, paramsMap);
        JsonNode jsonNode = HttpUtils.deserialize(responseBody);
        if (jsonNode.get("code").asInt() == 0) {
            String keyValue = jsonNode.get("result").get("keyValue").asText();
            if (StringUtils.isEmpty(keyValue)) {
                throw ConsoleExceptionUtils.message("Empty key value from KMS");
            }
            tokenSecretKeyCache.put(accessToken, keyValue);
            return keyValue;
        } else {
            logger.error(
                    "Error getServiceKey from KMS, url:{}, token:{}, response:{}", kmsUrl, accessToken, responseBody
            );
            throw ConsoleExceptionUtils.message("Error getServiceKey from KMS");
        }
    }

    @Override
    // query-pwd?token={token}&appid={appid}&herald-token={herald-token}&type={type}
    public Account getAccountInfo(String accessToken) {
        // get keyValue from kms via https request, should be loaded before generate metaInfo
        if (tokenAccountCache.containsKey(accessToken)) {
            return tokenAccountCache.get(accessToken);
        }
        String envStr = EnvUtils.getEnvStr();
        String kmsUrl = consoleConfig.getKmsUrl(envStr);
        Map<String, Object> paramsMap = Maps.newHashMap();
        paramsMap.put("token", accessToken);
        paramsMap.put("appid", Foundation.app().getAppId());
        paramsMap.put("herald-token", heraldService.getLocalHeraldToken());
        String queryParam= "/query-pwd?token={token}&appid={appid}&herald-token={herald-token}";
        String responseBody = HttpUtils.get(kmsUrl + queryParam, String.class, paramsMap);
        JsonNode jsonNode = HttpUtils.deserialize(responseBody);
        if (jsonNode.get("code").asInt() == 0) {
            String user = jsonNode.get("result").get("pwdAccount").asText();
            String pwd = jsonNode.get("result").get("pwdValue").asText();
            if (StringUtils.isEmpty(user) || StringUtils.isEmpty(pwd)) {
                throw ConsoleExceptionUtils.message("Empty user or pwd value from KMS");
            }
            Account account = new Account(user, pwd);
            tokenAccountCache.put(accessToken, account);
            return account;
        } else {
            logger.error(
                    "Error getAccountInfo from KMS, url:{}, token:{}, response:{}", kmsUrl, accessToken, responseBody
            );
            throw ConsoleExceptionUtils.message("Error getAccountInfo from KMS");
        }
    }


}
