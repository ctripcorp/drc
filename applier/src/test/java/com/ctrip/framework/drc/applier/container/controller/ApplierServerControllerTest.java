package com.ctrip.framework.drc.applier.container.controller;

import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * @Author limingdong
 * @create 2020/6/16
 */
public class ApplierServerControllerTest {

    @InjectMocks
    private ApplierServerController applierServerController;

    @Mock
    private ApplierServerContainer serverContainer;

    private MockMvc mockMvc;

    private static final String REGISTRY_KEY = "ut_cluster_name";

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.standaloneSetup(applierServerController).build();
    }

    @Test
    public void testDeleteFalse() throws Exception {
        Mockito.doNothing().when(serverContainer).removeServer(REGISTRY_KEY, false);
        this.mockMvc.perform(MockMvcRequestBuilders
                .delete("/appliers/{registryKey}/{delete}", REGISTRY_KEY, false)
                .contentType(MediaType.APPLICATION_JSON))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.status").value("0"))
                .andExpect(jsonPath("$.message").value("handle success"))
                .andExpect(jsonPath("$.data").value("true"));

        verify(serverContainer, times(1)).removeServer(REGISTRY_KEY, false);
        verify(serverContainer, times(0)).removeServer(REGISTRY_KEY, true);
    }

    @Test
    public void testDeleteTrue() throws Exception {
        Mockito.doNothing().when(serverContainer).removeServer(REGISTRY_KEY, true);
        this.mockMvc.perform(MockMvcRequestBuilders
                .delete("/appliers/{registryKey}/", REGISTRY_KEY)
                .contentType(MediaType.APPLICATION_JSON))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json;charset=UTF-8"))
                .andExpect(jsonPath("$.status").value("0"))
                .andExpect(jsonPath("$.message").value("handle success"))
                .andExpect(jsonPath("$.data").value("true"));

        verify(serverContainer, times(1)).removeServer(REGISTRY_KEY, true);
        verify(serverContainer, times(0)).removeServer(REGISTRY_KEY, false);
    }

}