package com.ctrip.framework.drc.service.console;

import com.ctrip.infosec.sso.client.principal.AttributePrincipal;
import com.ctrip.infosec.sso.client.util.AssertionHolder;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.ServletRequest;

import java.util.Map;

import static org.junit.Assert.*;

public class CtripSSOFilterWithDegradeSwitchTest {

    @Test
    public void testSetSSODegrade() {
        CtripSSOFilterWithDegradeSwitch ctripSSOFilterWithDegradeSwitch = new CtripSSOFilterWithDegradeSwitch(true);
        Boolean ssoDegradeStatus = ctripSSOFilterWithDegradeSwitch.setSSODegrade(true);
        Assert.assertEquals(true,ssoDegradeStatus);


        ctripSSOFilterWithDegradeSwitch.setDefaultUser();
        AttributePrincipal principal = AssertionHolder.getAssertion().getPrincipal();
        // 获取其他的登陆信息如下：
        Map map=principal.getAttributes();
        String name = (String) map.get("name");
        Assert.assertEquals("admin" , name);
    }
    
}