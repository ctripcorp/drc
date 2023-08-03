package com.ctrip.framework.drc.console.controller.v2;

import com.alibaba.arthas.deps.org.slf4j.Logger;
import com.alibaba.arthas.deps.org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaReplicationServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.console.vo.display.v2.MhaReplicationVo;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.PageResult;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


//@WebMvcTest(controllers = MhaReplicationController.class)
@RunWith(MockitoJUnitRunner.class)
public class MhaReplicationControllerTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());


    private MockMvc mvc;

    @InjectMocks
    private MhaReplicationController controller;

    @Mock
    private MhaServiceV2 mhaServiceV2;

    @Mock
    private MhaReplicationServiceV2 mhaReplicationServiceV2;

    @Mock
    private MetaInfoServiceV2 metaInfoServiceV2;

    @Before
    public void setUp() {
        this.mvc = MockMvcBuilders.standaloneSetup(controller).build();

        // mock data
        List<MhaReplicationTbl> mhaReplicationTbls = JSON.parseArray("[{\"id\":0,\"srcMhaId\":1,\"dstMhaId\":2},{\"id\":1,\"srcMhaId\":2,\"dstMhaId\":1}]", MhaReplicationTbl.class);
        List<MhaTblV2> list = JSON.parseArray("[{\"id\":1,\"mhaName\":\"mha1\",\"dcId\":1,\"buId\":1},{\"id\":2,\"mhaName\":\"mha2\",\"dcId\":1,\"buId\":1}]", MhaTblV2.class);
        Map<Long, MhaTblV2> map = list.stream().collect(Collectors.toMap(MhaTblV2::getId, e -> e));

        List<DcDo> dcDos = JSON.parseArray("[{\"dcId\":1,\"dcName\":\"test\",\"regionId\":1,\"regionName\":\"test\"}]", DcDo.class);
        Mockito.when(metaInfoServiceV2.queryAllDcWithCache()).thenReturn(dcDos);
        List<BuTbl> buTbls = JSON.parseArray("[{\"id\":3,\"buName\":\"BBZ\",\"deleted\":0,\"createTime\":1606209449710,\"datachangeLasttime\":1606209449710},{\"id\":4,\"buName\":\"IBU\",\"deleted\":0,\"createTime\":1606209670944,\"datachangeLasttime\":1606209670944},{\"id\":5,\"buName\":\"FX\",\"deleted\":0,\"createTime\":1618482818623,\"datachangeLasttime\":1618482818623},{\"id\":6,\"buName\":\"MBU\",\"deleted\":0,\"createTime\":1620897009325,\"datachangeLasttime\":1620897009325},{\"id\":7,\"buName\":\"BBZ3\",\"deleted\":0,\"createTime\":1621575183327,\"datachangeLasttime\":1621575183327},{\"id\":8,\"buName\":\"TRN\",\"deleted\":0,\"createTime\":1622514029050,\"datachangeLasttime\":1622514029050},{\"id\":9,\"buName\":\"PUB\",\"deleted\":0,\"createTime\":1645683319914,\"datachangeLasttime\":1645683319914},{\"id\":11,\"buName\":\"BBZTEST\",\"deleted\":0,\"createTime\":1655886120549,\"datachangeLasttime\":1655886120549},{\"id\":12,\"buName\":\"MKT\",\"deleted\":0,\"createTime\":1672817827782,\"datachangeLasttime\":1672817827782},{\"id\":13,\"buName\":\"GS\",\"deleted\":0,\"createTime\":1679900615228,\"datachangeLasttime\":1679900615228},{\"id\":14,\"buName\":\"FLT\",\"deleted\":0,\"createTime\":1679916177111,\"datachangeLasttime\":1679916177111},{\"id\":15,\"buName\":\"TOUR\",\"deleted\":0,\"createTime\":1688623962087,\"datachangeLasttime\":1688623962087}]", BuTbl.class);
        Mockito.when(metaInfoServiceV2.queryAllBuWithCache()).thenReturn(buTbls);


        Mockito.when(mhaReplicationServiceV2.queryRelatedReplications(Lists.newArrayList(1L))).thenReturn(mhaReplicationTbls);
        Mockito.when(mhaServiceV2.queryMhaByIds(Lists.newArrayList(1L, 2L))).thenReturn(map);

        Mockito.when(mhaReplicationServiceV2.queryRelatedReplications(Lists.newArrayList(3L))).thenReturn(Collections.emptyList());

        MhaReplicationQuery query = new MhaReplicationQuery();
        Mockito.when(mhaReplicationServiceV2.queryByPage(query)).thenReturn(PageResult.newInstance(mhaReplicationTbls, 1, 10, 2));

        Mockito.when(mhaServiceV2.query("mha3",null,null)).thenReturn(Collections.emptyMap());

    }

    @Test
    public void testQueryRelatedEmptyInput() throws Exception {
        MvcResult relatedMhaId = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v2/replication/queryMhaRelated")
                        .param("relatedMhaId", "")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
        ApiResult<List<MhaReplicationVo>> apiResult = JSON.parseObject(relatedMhaId.getResponse().getContentAsString(), ApiResult.class);
        Assert.assertNull(apiResult.getData());
        Assert.assertEquals(1L, apiResult.getStatus().longValue());
        Assert.assertTrue(apiResult.getMessage().contains("请求参数异常"));
    }

    @Test
    public void testQueryRelatedNormalInput() throws Exception {
        MvcResult relatedMhaId = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v2/replication/queryMhaRelated")
                        .param("relatedMhaId", "1")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
        ApiResult<List<MhaReplicationVo>> apiResult = JSON.parseObject(relatedMhaId.getResponse().getContentAsString(), ApiResult.class);

        Assert.assertEquals(2, apiResult.getData().size());
        Assert.assertEquals(0, apiResult.getStatus().longValue());
        Assert.assertTrue(apiResult.getMessage().contains("handle success"));
    }

    @Test
    public void testQueryRelatedEmptyResult() throws Exception {
        MvcResult relatedMhaId = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v2/replication/queryMhaRelated")
                        .param("relatedMhaId", "3")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
        ApiResult<List<MhaReplicationVo>> apiResult = JSON.parseObject(relatedMhaId.getResponse().getContentAsString(), ApiResult.class);

        Assert.assertEquals(0, apiResult.getData().size());
        Assert.assertEquals(0, apiResult.getStatus().longValue());
        Assert.assertTrue(apiResult.getMessage().contains("handle success"));
    }

    @Test
    public void testQueryPageEmptyInput() throws Exception {
        MvcResult relatedMhaId = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v2/replication/query")
                        .param("pageIndex","1")
                        .param("pageSize","20")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
        ApiResult apiResult = JSON.parseObject(relatedMhaId.getResponse().getContentAsString(), ApiResult.class);
        PageResult pageResult = ((JSONObject) apiResult.getData()).toJavaObject(PageResult.class);

        Assert.assertEquals(2, pageResult.getData().size());
        Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getCode(), apiResult.getStatus().longValue());
    }

    @Test
    public void testQueryPageNormalInput() throws Exception {
        MvcResult relatedMhaId = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v2/replication/query")
                        .param("pageIndex","1")
                        .param("pageSize","20")
                        .param("srcMha.name","mha3")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
        ApiResult apiResult = JSON.parseObject(relatedMhaId.getResponse().getContentAsString(), ApiResult.class);
        PageResult pageResult = ((JSONObject) apiResult.getData()).toJavaObject(PageResult.class);

        Assert.assertEquals(0, pageResult.getData().size());
        Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getCode(), apiResult.getStatus().longValue());
    }

}