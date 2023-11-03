package com.ctrip.framework.drc.service.console.web.filter;

import com.ctrip.infosec.sso.client.CtripSSOFilter;
import com.ctrip.infosec.sso.client.principal.AssertionImpl;
import com.ctrip.infosec.sso.client.principal.AttributePrincipalImpl;
import com.ctrip.infosec.sso.client.util.AssertionHolder;
import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.config.ConfigChangeListener;
import com.ctrip.xpipe.utils.VisibleForTesting;
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

public class CtripSSOFilterWithDegradeSwitch extends CtripSSOFilter implements ConfigChangeListener {
    
    private static final Logger logger = LoggerFactory.getLogger(CtripSSOFilterWithDegradeSwitch.class);
    private final String DEFAULT_SSO_DEGRADE_USERNAME = "admin";
    private final String SSO_DEGRADE_SWITCH = "sso.degrade.switch";
    private final String DEFAULT_SSO_DEGRADE_SWITCH = "off";

    private Config config;
    private boolean ssoDegrade = false;
    
    public CtripSSOFilterWithDegradeSwitch() {
        this.config = Config.DEFAULT;
        config.addConfigChangeListener(this);
    }
    
    //only for unitTest
    public CtripSSOFilterWithDegradeSwitch (Boolean ssoDegrade) {
        this.ssoDegrade = ssoDegrade;
    }
    
    //http post to change degrade status
    public Boolean setSSODegrade(Boolean ssoDegrade) {
        this.ssoDegrade = ssoDegrade;
        return this.ssoDegrade;
    }
    
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        super.init(filterConfig);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (ssoDegrade || getProperty(SSO_DEGRADE_SWITCH, DEFAULT_SSO_DEGRADE_SWITCH).equalsIgnoreCase("on")) {
            setDefaultUser();
            chain.doFilter(request,response);
        } else {
            super.doFilter(request,  response,  chain);
        }
    }
    
    @VisibleForTesting
    protected void setDefaultUser() {
        logger.info("[[switch=ssoDegrade]] ssoDegrade switch turn on");
        Map<String, String> userAttr = Maps.newHashMap();
        userAttr.put("name",DEFAULT_SSO_DEGRADE_USERNAME);
        AssertionImpl assertion = new AssertionImpl(new AttributePrincipalImpl(DEFAULT_SSO_DEGRADE_USERNAME, userAttr));
        AssertionHolder.setAssertion(assertion);
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
