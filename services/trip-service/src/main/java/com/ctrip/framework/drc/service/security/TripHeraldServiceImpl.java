package com.ctrip.framework.drc.service.security;

import com.ctrip.framework.drc.core.http.DynamicConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.service.security.HeraldService;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.sysdev.herald.reviewclient.ReviewClient;
import com.ctrip.sysdev.herald.reviewclient.dto.TokenPayload;
import com.ctrip.sysdev.herald.reviewclient.exception.InvalidHeraldTokenException;
import com.ctrip.sysdev.herald.tokenlib.Token;
import com.google.common.collect.Sets;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * @ClassName TripHeraldServiceImpl
 * @Author haodongPan
 * @Date 2024/5/31 11:03
 * @Version: $
 */
public class TripHeraldServiceImpl implements HeraldService {
    
    // key: heraldToken-ip
    private final Set<String> keyAuthorized = new ConcurrentHashSet<>();
    private final Set<String> appIdsAuthorized = Sets.newHashSet("100023928","100025243");
    private final int CONNECTION_TIMEOUT = 2000;
    
    private final Logger logger = LoggerFactory.getLogger(TripHeraldServiceImpl.class);
    private DynamicConfig dynamicConfig = DynamicConfig.getInstance();
    
    @Override
    public String getLocalHeraldToken() {
        return Token.getTokenString();
    }

    @Override
    public boolean heraldValidate(String heraldToken) {
        ServletRequestAttributes requestAttributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        HttpServletRequest request = requestAttributes.getRequest();
        String srcIp = request.getHeader("X-Forwarded-For");
        try {
            if (DynamicConfig.getInstance().shouldForceHeraldTokenCheck()) {
                return doHeraldValidate(heraldToken, srcIp);
            } else {
                if (!StringUtils.isEmpty(heraldToken)) { 
                    return doHeraldValidate(heraldToken, srcIp);
                } else { // heraldToken is empty, pass for old version CM request
                    return true; 
                }
            }
        }  catch (Exception e) {
            logger.error("heraldToken validate exception downgrade, srcIp: {},heraldToken:{}", srcIp,heraldToken, e);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.herald.exception",srcIp+"|"+heraldToken);
            return true;
        }
    }
    
    private boolean doHeraldValidate(String heraldToken, String srcIp) {
        if (StringUtils.isBlank(heraldToken) || StringUtils.isBlank(srcIp)) {
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.herald.illegal",srcIp+"|"+heraldToken);
            logger.warn("Invalid herald token or srcIp: heraldToken={}, srcIp={}", heraldToken, srcIp);
            return false;
        }
        String key = heraldToken + "-" + srcIp;
        if (keyAuthorized.contains(key)) {
            return true;
        }

        String serviceUrl = dynamicConfig.getHeraldServerUrl();
        String authToken = dynamicConfig.getHeraldAuthToken();
        ReviewClient client = new ReviewClient(serviceUrl, authToken);
        client.setConnectTimeout(CONNECTION_TIMEOUT);
        try {
            TokenPayload tokenPayload = client.reviewTokenPayload(heraldToken, srcIp);
            boolean validateResult = srcIp.equals(tokenPayload.IP) && appIdsAuthorized.contains(tokenPayload.AppID);
            if (validateResult) {
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.herald.success",srcIp);
                keyAuthorized.add(key);
            } else {
                logger.warn("heraldToken validate fail, srcIp: {},heraldToken:{}", srcIp,heraldToken);
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.herald.fail",srcIp+"|"+heraldToken);
            }
            return validateResult;
        } catch (InvalidHeraldTokenException e) {
            logger.warn("Invalid herald token: " + heraldToken,srcIp,e);
            return false;
        }
    }
    

    @Override
    public int getOrder() {
        return 0;
    }
}
