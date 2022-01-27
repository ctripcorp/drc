package com.ctrip.framework.drc.service.console;

import com.ctrip.framework.drc.core.service.user.UserService;
import com.ctrip.infosec.sso.client.principal.AttributePrincipal;
import com.ctrip.infosec.sso.client.util.AssertionHolder;
import com.ctrip.xpipe.config.AbstractConfigBean;

import java.util.Map;

/**
 * @author wangjixin
 * @version 1.0
 * date: 2020-03-03
 */



public class UserServiceImpl extends AbstractConfigBean implements UserService {
    private static final String SSO_LOGOUT_URL = "sso.logout.url";

    @Override
    public String getInfo() {
        try {
            AttributePrincipal principal = AssertionHolder.getAssertion().getPrincipal();
            // 获取其他的登陆信息如下：
            Map map=principal.getAttributes();
            String name=(String) map.get("name");
            return name;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String getLogoutUrl() {
        try {
            String ssoLogoutUrl = getProperty(SSO_LOGOUT_URL);
            return ssoLogoutUrl;
        } catch (Exception e) {
            logger.warn("getInfo error",e);
            return null;
        }
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
