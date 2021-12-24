//package com.ctrip.framework.drc.console.controller;
//
//import com.ctrip.framework.drc.console.service.UserService;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//import org.mockito.InjectMocks;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//import org.springframework.http.MediaType;
//import org.springframework.test.web.servlet.MockMvc;
//import org.springframework.test.web.servlet.MvcResult;
//import org.springframework.test.web.servlet.setup.MockMvcBuilders;
//
//import static org.mockito.Mockito.when;
//import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
//
///**
// * Created by jixinwang on 2020/6/23
// */
//public class UserControllerTest {
//
//    private MockMvc mvc;
//
//    @InjectMocks
//    private UserController controller;
//
//    @Mock
//    private UserService userService;
//
//    @Before
//    public void setUp() {
//        /** initialization */
//        MockitoAnnotations.initMocks(this);
//        /** build mvc env */
//        mvc = MockMvcBuilders.standaloneSetup(controller).build();
//    }
//
//    @Test
//    public void testGetInfo() throws Exception {
//        String info = "test";
//        when(userService.getInfo()).thenReturn(info);
//        MvcResult mvcResult = mvc.perform(get("/api/drc/v1/user/current")
//                .accept(MediaType.APPLICATION_JSON))
//                .andReturn();
//        int status = mvcResult.getResponse().getStatus();
//        Assert.assertEquals(200, status);
//    }
//
//    @Test
//    public void testGetLogoutUrl() throws Exception {
//        String url = "testUrl";
//        when(userService.getLogoutUrl()).thenReturn(url);
//        MvcResult mvcResult = mvc.perform(get("/api/drc/v1/user/logout")
//                .accept(MediaType.APPLICATION_JSON))
//                .andReturn();
//        int status = mvcResult.getResponse().getStatus();
//        Assert.assertEquals(200, status);
//    }
//}
