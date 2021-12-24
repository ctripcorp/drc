package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.dao.entity.ClusterTbl;
import com.ctrip.framework.drc.console.pojo.Mha;
import com.ctrip.framework.drc.console.service.impl.ClusterTblServiceImpl;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.when;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-13
 */
public class ClusterControllerTest {

    private MockMvc mvc;

    @InjectMocks
    private ClusterController controller;

    @Mock
    private ClusterTblServiceImpl clusterTblService;

    @Mock
    private DalServiceImpl dalService;

    private List<ClusterTbl> clusterTbls;

    @Before
    public void setUp() {
        ClusterTbl clusterTbl1 = new ClusterTbl();
        clusterTbl1.setId(1L);
        clusterTbl1.setClusterName("cluster1");
        clusterTbl1.setClusterAppId(123456L);
        ClusterTbl clusterTbl2 = new ClusterTbl();
        clusterTbl2.setId(2L);
        clusterTbl2.setClusterName("cluster2");
        clusterTbl2.setClusterAppId(123456L);
        clusterTbls = new ArrayList<>() {{
            add(clusterTbl1);
            add(clusterTbl2);
        }};

        List<Mha> mhas = new ArrayList<>() {{
            add(new Mha("mha1", "shajq", new Mha.DbEndpoint("127.0.0.1", 8080, null), new ArrayList<>() {{
                add(new Mha.DbEndpoint("127.0.0.1", 8081, null));
            }}));
            add(new Mha("mha2", "shaoy", new Mha.DbEndpoint("127.0.0.2", 8080, null), new ArrayList<>() {{
                add(new Mha.DbEndpoint("127.0.0.2", 8081, null));
            }}));
        }};

        /** initialization */
        MockitoAnnotations.initMocks(this);
        /** build mvc env */
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
        when(clusterTblService.getClusters(1, 10)).thenReturn(clusterTbls);
        when(clusterTblService.getRecordsCount()).thenReturn(77);
        when(dalService.getMhasFromDal("test")).thenReturn(mhas);
    }

    @Test
    public void testGetClusters() throws Exception {
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/clusters/all/pageNo/1/pageSize/10").accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }

    @Test
    public void testGetClusterCount() throws Exception {
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/clusters/all/count").accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }

    @Test
    public void testGetMhas() throws Exception {
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/clusters/test/mhas").accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }
}
