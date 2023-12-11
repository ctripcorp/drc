package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DbClusterRetriever;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.pojo.Mha;
import com.ctrip.framework.drc.core.service.dal.DalClusterTypeEnum;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.ctrip.framework.foundation.Env;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.mockito.Mockito.when;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-14
 */
public class DalServiceImplTest {

    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private DalServiceImpl dalService;

    @Mock
    private DbClusterRetriever dbClusterRetriever;

    @Mock
    private DbClusterApiService dbClusterApiServiceImpl;
    
    @Mock
    private DomainConfig domainConfig;

    @Before
    public void setUp() throws Exception {
        String resultStr = "[{\"mhaName\":\"mha1\",\"zoneId\":\"shajq\",\"master\":{\"ip\":\"127.0.0.1\",\"port\":8080,\"readWeight\":null},\"slaves\":[{\"ip\":\"127.0.0.1\",\"port\":8081,\"readWeight\":null}]},{\"mhaName\":\"mha2\",\"zoneId\":\"shaoy\",\"master\":{\"ip\":\"127.0.0.2\",\"port\":8080,\"readWeight\":null},\"slaves\":[{\"ip\":\"127.0.0.2\",\"port\":8081,\"readWeight\":null}]}]";
        JsonNode mhaResult = new ObjectMapper().readTree(resultStr);
        List<Mha> mhas = new ArrayList<>() {{
            add(new Mha("mha1", "shajq", new Mha.DbEndpoint("127.0.0.1", 8080, null), new ArrayList<>() {{
                add(new Mha.DbEndpoint("127.0.0.1", 8081, null));
            }}));
            add(new Mha("mha2", "shaoy", new Mha.DbEndpoint("127.0.0.2", 8080, null), new ArrayList<>() {{
                add(new Mha.DbEndpoint("127.0.0.2", 8081, null));
            }}));
        }};

        MockitoAnnotations.openMocks(this);
        when(dbClusterRetriever.getMhasNode("test")).thenReturn(mhaResult);
        when(dbClusterRetriever.getMhas(mhaResult)).thenReturn(mhas);
    }

    @Test
    public void testGetMhasFromDal() {
        List<Mha> mhas = dalService.getMhasFromDal("test");
        for(Mha mha : mhas) {
            System.out.println(mha.toString());
        }
        Assert.assertNotEquals(0, mhas.size());
    }

    @Test
    public void testGetInstanceGroupsInfo() throws Exception {
        List<String> mhas = Arrays.asList("fat-fx-drc1", "fat-fx-drc2");
        String response = "{\"status\":200,\"message\":\"query clusters by mhaNames success.\",\"result\":[{\"mhaName\":\"fat-fx-drc1\",\"clusters\":[{\"clusterName\":\"bbzdrccameldb_dalcluster\",\"baseDbName\":\"bbzdrccameldb\",\"shards\":[{\"shardIndex\":0,\"dbName\":\"bbzdrccameldb\",\"zoneId\":\"ntgxh\"}]},{\"clusterName\":\"bbzdrcbenchmarkdb_dalcluster\",\"baseDbName\":\"bbzdrcbenchmarkdb\",\"shards\":[{\"shardIndex\":0,\"dbName\":\"bbzdrcbenchmarkdb\",\"zoneId\":\"ntgxh\"}]}]},{\"mhaName\":\"fat-fx-drc2\",\"clusters\":[{\"clusterName\":\"bbzdrccameldb_dalcluster\",\"baseDbName\":\"bbzdrccameldb\",\"shards\":[{\"shardIndex\":0,\"dbName\":\"bbzdrccameldb\",\"zoneId\":\"stgxh\"}]},{\"clusterName\":\"bbzdrcbenchmarkdb_dalcluster\",\"baseDbName\":\"bbzdrcbenchmarkdb\",\"shards\":[{\"shardIndex\":0,\"dbName\":\"bbzdrcbenchmarkdb\",\"zoneId\":\"stgxh\"}]}]}]}";
        String invalid_response1 = "{\"status\":500,\"message\":\"query clusters by mhaNames fail.\",\"result\":null}";
        String invalid_response2 = "{\"status\":500,\"message\":\"query clusters by mhaNames fail.\",\"result\":\"no info\"}";

        try {
            Mockito.doReturn(objectMapper.readTree(response).get("result")).when(dbClusterApiServiceImpl).getInstanceGroupsInfo(Mockito.any(),Mockito.any());
            Map<String, String> instanceGroupsInfo = dalService.getInstanceGroupsInfo(mhas, Env.FAT);
            Assert.assertEquals(2, instanceGroupsInfo.size());
            Assert.assertTrue(instanceGroupsInfo.containsKey("fat-fx-drc1"));
            String s = instanceGroupsInfo.get("fat-fx-drc1");
            String[] split = s.split(",");
            Assert.assertTrue(ArrayUtils.contains(split, "bbzdrcbenchmarkdb_dalcluster"));
            Assert.assertTrue(ArrayUtils.contains(split, "bbzdrccameldb_dalcluster"));
            Assert.assertTrue(instanceGroupsInfo.containsKey("fat-fx-drc2"));
            s = instanceGroupsInfo.get("fat-fx-drc2");
            split = s.split(",");
            Assert.assertTrue(ArrayUtils.contains(split, "bbzdrcbenchmarkdb_dalcluster"));
            Assert.assertTrue(ArrayUtils.contains(split, "bbzdrccameldb_dalcluster"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Mockito.doReturn(objectMapper.readTree(invalid_response1).get("result")).when(dbClusterApiServiceImpl).getInstanceGroupsInfo(Mockito.any(),Mockito.any());
            Map<String, String> instanceGroupsInfo = dalService.getInstanceGroupsInfo(mhas, Env.FAT);
            Assert.assertEquals(0, instanceGroupsInfo.size());
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Mockito.doReturn(objectMapper.readTree(invalid_response2).get("result")).when(dbClusterApiServiceImpl).getInstanceGroupsInfo(Mockito.any(),Mockito.any());
            Map<String, String> instanceGroupsInfo = dalService.getInstanceGroupsInfo(mhas, Env.FAT);
            Assert.assertEquals(0, instanceGroupsInfo.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetMhaList() {
        String response = "{\"status\":200,\"message\":\"query instance group success.\",\"result\":[{\"mhaName\":\"dalfat\",\"zoneId\":\"ntgxh\",\"type\":\"mha\",\"master\":{\"ip\":\"10.2.63.122\",\"port\":55111,\"idc\":\"ntgxh\"},\"slaves\":[{\"ip\":\"10.2.61.170\",\"port\":55111,\"idc\":\"ntgxh\"}]},{\"mhaName\":\"fat-arc-pub1-4\",\"zoneId\":\"ntgxh\",\"type\":\"mha\",\"master\":{\"ip\":\"10.5.20.144\",\"port\":55111,\"idc\":\"ntgxh\"},\"slaves\":[{\"ip\":\"10.5.22.81\",\"port\":55111,\"idc\":\"ntgxh\"}]},{\"mhaName\":\"fat-arc-pub2-4\",\"zoneId\":\"ntgxh\",\"type\":\"mha\",\"master\":{\"ip\":\"10.5.22.108\",\"port\":55111,\"idc\":\"ntgxh\"},\"slaves\":[{\"ip\":\"10.5.22.109\",\"port\":55111,\"idc\":\"ntgxh\"}]}]}";
        try {
            Mockito.doReturn(objectMapper.readTree(response).get("result")).when(dbClusterApiServiceImpl).getMhaList(Mockito.any());
            Map<String, MhaInstanceGroupDto> mhaList = dalService.getMhaList(Env.FAT);
            Assert.assertEquals(3, mhaList.size());
            for(MhaInstanceGroupDto dto : mhaList.values()) {
                System.out.println(dto);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetDc() {
        
        String response = "{\"status\":200,\"message\":\"query instance group success.\",\"result\":[{\"mhaName\":\"dalfat\",\"zoneId\":\"ntgxh\",\"type\":\"mha\",\"master\":{\"ip\":\"10.2.63.122\",\"port\":55111,\"idc\":\"ntgxh\"},\"slaves\":[{\"ip\":\"10.2.61.170\",\"port\":55111,\"idc\":\"ntgxh\"}]},{\"mhaName\":\"fat-arc-pub1-4\",\"zoneId\":\"ntgxh\",\"type\":\"mha\",\"master\":{\"ip\":\"10.5.20.144\",\"port\":55111,\"idc\":\"ntgxh\"},\"slaves\":[{\"ip\":\"10.5.22.81\",\"port\":55111,\"idc\":\"ntgxh\"}]},{\"mhaName\":\"fat-arc-pub2-4\",\"zoneId\":\"ntgxh\",\"type\":\"mha\",\"master\":{\"ip\":\"10.5.22.108\",\"port\":55111,\"idc\":\"ntgxh\"},\"slaves\":[{\"ip\":\"10.5.22.109\",\"port\":55111,\"idc\":\"ntgxh\"}]}]}";
        try {
            Mockito.doReturn(objectMapper.readTree(response).get("result")).when(dbClusterApiServiceImpl).getMhaList(Mockito.any());
            Assert.assertEquals("ntgxh", dalService.getDc("dalfat", Env.FAT));
            Assert.assertNull(dalService.getDc("nosuchmha", Env.FAT));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetDalClusterInfoWrappers() {
        Set<String> dalClusterNames = Sets.newHashSet("bbzdrcbenchmarkdb_dalcluster");
        String response = "{\"status\":200,\"message\":\"query cluster success.\",\"result\":{\"clusterName\":\"bbzdrcbenchmarkdb_dalcluster\",\"type\":\"normal\",\"unitStrategyId\":100,\"unitStrategyName\":\"mock\",\"shardStrategies\":null,\"idGenerators\":null,\"freshnessThresholdSecond\":null,\"dbCategory\":\"mysql\",\"enabled\":true,\"released\":true,\"releaseVersion\":48,\"createTime\":null,\"lastReleaseTime\":null,\"shards\":[{\"shardIndex\":0,\"dbName\":\"bbzdrcbenchmarkdb\",\"zones\":[{\"zoneId\":\"ntgxh\",\"active\":true,\"master\":{\"domain\":\"bbzdrcbenchmark.mysql.db.fat.ntgxh.qa.nt.ctripcorp.com\",\"port\":55111,\"instance\":{\"ip\":\"10.2.72.247\",\"port\":55111,\"tags\":null},\"instances\":null},\"slave\":{\"domain\":null,\"port\":null,\"instance\":null,\"instances\":[{\"ip\":\"10.2.72.230\",\"port\":55111,\"tags\":null}]},\"read\":null},{\"zoneId\":\"stgxh\",\"active\":false,\"master\":{\"domain\":\"bbzdrcbenchmark.mysql.db.fat.stgxh.qa.nt.ctripcorp.com\",\"port\":55111,\"instance\":{\"ip\":\"10.2.72.246\",\"port\":55111,\"tags\":null},\"instances\":null},\"slave\":{\"domain\":null,\"port\":null,\"instance\":null,\"instances\":[{\"ip\":\"10.2.72.248\",\"port\":55111,\"tags\":null}]},\"read\":null}],\"users\":[{\"username\":\"w_bbzdmark_89\",\"password\":null,\"permission\":\"write\",\"tag\":\"\",\"enabled\":true,\"titanKey\":\"bbzdrcbenchmarkdb_w\"}]}]}}";

        try {
            Mockito.doReturn(objectMapper.readTree(response).get("result")).when(dbClusterApiServiceImpl).getDalClusterInfo(Mockito.anyString(),Mockito.anyString());
            List<DalServiceImpl.DalClusterInfoWrapper> wrappers = dalService.getDalClusterInfoWrappers(dalClusterNames, "fat");
            Assert.assertNotNull(wrappers);
            Assert.assertEquals(1, wrappers.size());
            wrappers.forEach(wrapper -> {
                Assert.assertTrue(dalClusterNames.contains(wrapper.getDalClusterName()));
                Assert.assertTrue(DalClusterTypeEnum.NORMAL.getValue().equalsIgnoreCase(wrapper.getType()));
                Assert.assertNotEquals(0, wrapper.getZoneIds().size());
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetDbNames() throws Exception {
        List<String> mhaNames = Lists.newArrayList();
        mhaNames.add("testMhaName");

        String uri = "URI_getInstanceGroupsInfo";
        String INSTANCE_GROUP_RESULT = "{\n" +
                "\"status\": 200,\n" +
                "\"message\": \"query clusters by mhaNames success.\",\n" +
                "\"result\": [\n" +
                "{\n" +
                "\"mhaName\": \"testMhaName\",\n" +
                "\"clusters\": [\n" +
                "{\n" +
                "\"clusterName\": \"testMhaNamedb_dalcluster\",\n" +
                "\"baseDbName\": \"testMhaNamedb\",\n" +
                "\"shards\": [\n" +
                "{\n" +
                "\"shardIndex\": 0,\n" +
                "\"dbName\": \"testMhaName1db\",\n" +
                "\"zoneId\": \"shaoy\",\n" +
                "\"masterDomain\": \"testMhaName.ctrip.com\"\n" +
                "}\n" +
                "]\n" +
                "}\n" +
                "]\n" +
                "}\n" +
                "]\n" +
                "}";
        try {
            Mockito.doReturn(objectMapper.readTree(INSTANCE_GROUP_RESULT).get("result")).when(dbClusterApiServiceImpl).getInstanceGroupsInfo(Mockito.any(),Mockito.any());
            Map<String, Map<String, List<String>>> res = dalService.getDbNames(mhaNames, Env.FAT);
            Assert.assertEquals(res.size(), 1);
            Map<String, List<String>> cluster2Db = res.get("testMhaName");
            Assert.assertEquals(cluster2Db.size(), 1);
            List<String> dbs = cluster2Db.get("testMhaNamedb_dalcluster");
            Assert.assertEquals(dbs.size(), 1);
            Assert.assertTrue(dbs.contains("testMhaName1db"));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
