package com.ctrip.framework.drc.service.console;

import com.ctrip.infosec.sso.client.CtripSSOFilter;
import com.ctrip.infosec.sso.client.principal.Assertion;
import com.ctrip.infosec.sso.client.principal.AssertionImpl;
import com.ctrip.infosec.sso.client.principal.AttributePrincipalImpl;
import com.ctrip.infosec.sso.client.util.AssertionHolder;
import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.config.ConfigChangeListener;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.FilterConfig;
import java.io.IOException;
import java.util.Map;

/**
 * @ClassName DegradeFilter
 * @Author haodongPan
 * @Date 2022/3/7 11:15
 * @Version: $
 */

public class CtripSSOFilterWithDegradeSwtich extends CtripSSOFilter implements ConfigChangeListener {
    
    private static final Logger logger = LoggerFactory.getLogger(CtripSSOFilterWithDegradeSwtich.class);
    private final String SSO_DEGRADE_SWITCH = "sso.degrade.switch";
    private final String DEFAULT_SSO_DEGRADE_SWITCH = "off";
    private final String SSO_DEGRADE_USERNAME = "sso.degrade.username";
    private final String DEFAULT_SSO_DEGRADE_USERNAME = "admin";
    
    private Config config;
    
    public CtripSSOFilterWithDegradeSwtich() {
        this.config = Config.DEFAULT;
        config.addConfigChangeListener(this);
    }
    
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        super.init(filterConfig);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        //before Filter 
        String degradeSwitch = getProperty(SSO_DEGRADE_SWITCH, DEFAULT_SSO_DEGRADE_SWITCH);
        if (degradeSwitch.equalsIgnoreCase("on")) {
            logger.info("[[switch=ssoDegrade]] ssoDegrade switch turn on");
            Map<String, String> userAttr = Maps.newHashMap();
            String degradeUsername = getProperty(SSO_DEGRADE_USERNAME, DEFAULT_SSO_DEGRADE_USERNAME);
            userAttr.put("name",degradeUsername);
            AssertionImpl assertion = new AssertionImpl(new AttributePrincipalImpl(degradeUsername, userAttr));
            AssertionHolder.setAssertion(assertion);
            chain.doFilter(request,response);
        } else {
            super.doFilter(request,  response,  chain);
        }
    }

    @Override
    public void destroy() {
        super.destroy();
    }
    
    protected String getProperty(String key, String defaultValue){
        return config.get(key, defaultValue);
    }
    
    @Override
    public void onChange(String key, String oldValue, String newValue) {
        //String value change trigger already done in mapConfig
    }
}
