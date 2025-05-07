package com.ctrip.framework.drc.core.service.inquirer;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import java.util.List;


public class ApplierInfoInquirerTest {

    @InjectMocks
    private ApplierInfoInquirer infoInquirer;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testUrl() {
        String ipAndPort = "127.0.0.10:8080";
        String url = infoInquirer.getUrl(ipAndPort);
        Assert.assertEquals("http://127.0.0.10:8080/appliers/info/all", url);
    }

    @Test
    public void testParseJson() {
        ApiResult apiResult = JsonUtils.fromJson(getJson(), ApiResult.class);
        List<ApplierInfoDto> applierInfoDtos = infoInquirer.parseData(apiResult);
        Assert.assertEquals(2, applierInfoDtos.size());
        ApplierInfoDto dto = applierInfoDtos.get(0);
        Assert.assertEquals("phd_test3_dalcluster.phd_test3._drc_mq", dto.getRegistryKey());
        Assert.assertEquals(true, dto.getMaster());
        Assert.assertEquals("10.118.1.118", dto.getIp());
        Assert.assertEquals(Integer.valueOf(8080), dto.getPort());
        Assert.assertEquals("10.118.1.137", dto.getReplicatorIp());

        dto = applierInfoDtos.get(1);
        Assert.assertEquals("gs_mkt_pub4_dalcluster.fat-gs-mkt-pub4._drc_mq", dto.getRegistryKey());
        Assert.assertEquals(true, dto.getMaster());
        Assert.assertEquals("10.118.1.118", dto.getIp());
        Assert.assertEquals(Integer.valueOf(8080), dto.getPort());
        Assert.assertEquals("10.118.1.142", dto.getReplicatorIp());
    }

    public String getJson() {
        return "{\n" +
                "    \"status\": 0,\n" +
                "    \"message\": \"handle success\",\n" +
                "    \"data\": [\n" +
                "        {\n" +
                "            \"registryKey\": \"phd_test3_dalcluster.phd_test3._drc_mq\",\n" +
                "            \"master\": true,\n" +
                "            \"ip\": \"10.118.1.118\",\n" +
                "            \"port\": 8080,\n" +
                "            \"replicatorIp\": \"10.118.1.137\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"registryKey\": \"gs_mkt_pub4_dalcluster.fat-gs-mkt-pub4._drc_mq\",\n" +
                "            \"master\": true,\n" +
                "            \"ip\": \"10.118.1.118\",\n" +
                "            \"port\": 8080,\n" +
                "            \"replicatorIp\": \"10.118.1.142\"\n" +
                "        }\n" +
                "    ],\n" +
                "    \"pageReq\": null\n" +
                "}";
    }

}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme