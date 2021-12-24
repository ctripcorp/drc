package com.ctrip.framework.drc.core.http;

import com.ctrip.xpipe.config.AbstractConfigBean;

/**
 * @ClassName AuthorityConfig
 * @Author haodongPan
 * @Date 2021/12/13 14:05
 * @Version: $
 */
public class AuthorityConfig extends AbstractConfigBean {
    private static final String X_ACCESS_TOKEN = "x.access.token";
    private static final String DEFAULT_X_ACCESS_TOKEN = "";

    public String getXAccessToken() {
        return getProperty(X_ACCESS_TOKEN,DEFAULT_X_ACCESS_TOKEN);
    }
    
    private AuthorityConfig(){
        super();
    };
    
    public static AuthorityConfig getInstance(){
        return AuthorityConfigHolder.INSTANCE;
    }
    
    private static class AuthorityConfigHolder {
        private static AuthorityConfig INSTANCE = new AuthorityConfig();
    }
    
}
