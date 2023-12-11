package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.controller.v1.AccessController;
import com.ctrip.framework.drc.console.service.SSOService;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * @author maojiawei
 * @version 1.0
 * date: 2020-07-28
 */
public class AccessControllerTest extends AbstractControllerTest {

    @InjectMocks
    private AccessController accessController;

  

    @Mock
    private SSOService ssoServiceImpl;

    @Before
    public void setup() throws Throwable {
        /** initialization */
        MockitoAnnotations.initMocks(this);
        /** build mvc env */
        mvc = MockMvcBuilders.standaloneSetup(accessController).build();
    }
    
    @Test
    public  void testSSODegrade() throws Exception {
        Mockito.doReturn(ApiResult.getSuccessInstance(null)).when(ssoServiceImpl).degradeAllServer(true);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/access/sso/degrade/switch/true")
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        Assert.assertEquals(200, mvcResult.getResponse().getStatus());

        Mockito.doReturn(ApiResult.getSuccessInstance(null)).when(ssoServiceImpl).setDegradeSwitch(true);
         mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/access/sso/degrade/notify/true")
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        Assert.assertEquals(200, mvcResult.getResponse().getStatus());
    }
}
