package com.ctrip.framework.drc.messenger.container.controller;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerConfigDto;
import com.ctrip.framework.drc.messenger.container.MqServerContainer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

/**
 * Created by shiruixin
 * 2024/11/8 16:36
 */
public class MqServerControllerTest {
    @InjectMocks
    private MqServerController mqServerController;

    @Mock
    private MqServerContainer serverContainer;

    private MockMvc mockMvc;

    private static final String REGISTRY_KEY = "ut_cluster_name";

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        this.mockMvc = MockMvcBuilders.standaloneSetup(mqServerController).build();
    }

    @Test
    public void testDeleteFalse() throws Exception {
        Mockito.doNothing().when(serverContainer).removeServer(REGISTRY_KEY, false);
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders
                .delete("/appliers/{registryKey}/{delete}", REGISTRY_KEY, false)
                .contentType(MediaType.APPLICATION_JSON);
        this.mockMvc.perform(requestBuilder)
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

    @Test
    public void testRegisterAddDelete() throws Exception {
        MessengerConfigDto dto = new MessengerConfigDto();
        DBInfo target = new DBInfo();
        target.setMhaName("dstMha");
        dto.setTarget(target);

        InstanceInfo replicator = new InstanceInfo();
        replicator.setMhaName("srcMha");
        dto.setReplicator(replicator);

        dto.setCluster("dstMha_cluster");
        dto.setApplyMode(ApplyMode.transaction_table.getType());
        Assert.assertEquals("dstMha_cluster.dstMha.srcMha", dto.getRegistryKey());


        Mockito.when(serverContainer.registerServer(Mockito.eq(dto.getRegistryKey()))).thenReturn(null);
        Mockito.when(serverContainer.addServer(Mockito.eq(dto))).thenReturn(true);
        Mockito.doNothing().when(serverContainer).removeServer(Mockito.eq(dto.getRegistryKey()), Mockito.eq(true));

        // work success
        ApiResult<Boolean> register = mqServerController.register(dto);
        Assert.assertTrue(register.getData());

        System.out.println("test");
        ApiResult<Boolean> add = mqServerController.post(dto);
        Assert.assertTrue(add.getData());

        ApiResult<Boolean> delete = mqServerController.remove(dto.getRegistryKey(), java.util.Optional.of(true));
        Assert.assertTrue(delete.getData());

        Thread.sleep(200);

        verify(serverContainer, times(1)).registerServer(Mockito.eq(dto.getRegistryKey()));
        verify(serverContainer, times(1)).addServer(Mockito.eq(dto));
        verify(serverContainer, times(1)).removeServer(Mockito.eq(dto.getRegistryKey()), Mockito.eq(true));

        // work fail
        Mockito.when(serverContainer.registerServer(Mockito.eq(dto.getRegistryKey()))).thenThrow(new RuntimeException("mock exception"));
        Mockito.when(serverContainer.addServer(Mockito.eq(dto))).thenThrow(new RuntimeException("mock exception"));
        Mockito.doThrow(new RuntimeException("mock exception")).when(serverContainer).removeServer(Mockito.eq(dto.getRegistryKey()), Mockito.eq(true));

        register = mqServerController.register(dto);
        Assert.assertTrue(register.getData());

        add = mqServerController.post(dto);
        Assert.assertTrue(add.getData());

        delete = mqServerController.register(dto);
        Assert.assertTrue(delete.getData());

        Thread.sleep(4000);

        verify(serverContainer, atLeast(3)).registerServer(Mockito.eq(dto.getRegistryKey()));
        verify(serverContainer, times(1)).addServer(Mockito.eq(dto));
        verify(serverContainer, times(1)).removeServer(Mockito.eq(dto.getRegistryKey()), Mockito.eq(true));
    }
}