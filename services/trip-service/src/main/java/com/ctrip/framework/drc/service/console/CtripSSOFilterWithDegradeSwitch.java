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

public class CtripSSOFilterWithDegradeSwitch extends CtripSSOFilter  {
    
    private static final Logger logger = LoggerFactory.getLogger(CtripSSOFilterWithDegradeSwitch.class);
    private final String DEFAULT_SSO_DEGRADE_USERNAME = "admin";
    
    private boolean ssoDegrade = false;
    
    public CtripSSOFilterWithDegradeSwitch() {
    }
    
    //only for unitTest
    public CtripSSOFilterWithDegradeSwitch (Boolean ssoDegrade) {
        this.ssoDegrade = ssoDegrade;
    }
    
    public Boolean setSSODegrade(Boolean ssoDegrade) {
        this.ssoDegrade = ssoDegrade;
        return this.ssoDegrade;
    }
    
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        if (ssoDegrade) {
            // no need to init permissionBean
            return;
        }
        super.init(filterConfig);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (ssoDegrade) {
            setDefaultUser();
            chain.doFilter(request,response);
        } else {
            super.doFilter(request,  response,  chain);
        }
    }
    
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
    
}
