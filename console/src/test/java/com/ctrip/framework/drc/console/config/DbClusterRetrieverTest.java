package com.ctrip.framework.drc.console.config;

import com.ctrip.framework.drc.console.pojo.DalCluster;
import com.ctrip.framework.drc.console.pojo.Mha;
import com.ctrip.framework.drc.console.pojo.TableConfig;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-08
 */
public class DbClusterRetrieverTest {

    @InjectMocks
    private DbClusterRetriever dbClusterRetriever = new DbClusterRetriever();

    private JsonNode clusterResult;

    private JsonNode mhaResult;

    private ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private DomainConfig domainConfig;

    @Mock
    private DbClusterApiService dbClusterApiService;

    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);
        String resultStr = "[{\"clusterName\":\"testClusterName\",\"type\":\"testType\",\"unitStrategyId\":2,\"unitStrategyName\":\"testStrategyName\",\"shardStrategies\":null,\"idGenerators\":null,\"dbCategory\":\"mysql\",\"enabled\":true,\"released\":true,\"releaseVersion\":23,\"shards\":null}, {\"clusterName\":\"testClusterName2\",\"type\":\"testType\",\"unitStrategyId\":2,\"unitStrategyName\":\"testStrategyName\",\"shardStrategies\":null,\"idGenerators\":null,\"dbCategory\":\"mysql\",\"enabled\":true,\"released\":true,\"releaseVersion\":23,\"shards\":null}]";
        clusterResult = new ObjectMapper().readTree(resultStr);
        resultStr = "[{\"mhaName\":\"mha1\",\"zoneId\":\"shajq\",\"master\":{\"ip\":\"127.0.0.1\",\"port\":8080,\"readWeight\":null},\"slaves\":[{\"ip\":\"127.0.0.1\",\"port\":8081,\"readWeight\":null}]},{\"mhaName\":\"mha2\",\"zoneId\":\"shaoy\",\"master\":{\"ip\":\"127.0.0.2\",\"port\":8080,\"readWeight\":null},\"slaves\":[{\"ip\":\"127.0.0.2\",\"port\":8081,\"readWeight\":null}]}]";
        mhaResult = new ObjectMapper().readTree(resultStr);
        
    }

    @Test
    public void testGetDalClusterNames() {
        List<String> clusters = dbClusterRetriever.getDalClusterNames(clusterResult);
        System.out.println(clusters.size());
        Assert.assertNotNull(clusters);
        Assert.assertNotEquals(0, clusters.size());
    }

    @Test
    public void testGetDalClusters() {
        List<DalCluster> dalClusters = dbClusterRetriever.getDalClusters(clusterResult);
        Assert.assertNotNull(dalClusters);
        Assert.assertNotEquals(0, dalClusters.size());
    }

    @Test
    public void testGetMhas() {
        List<Mha> mhas = dbClusterRetriever.getMhas(mhaResult);
        for (Mha mha : mhas) {
            System.out.println(mha.toString());
        }
        Assert.assertNotNull(mhas);
        Assert.assertNotEquals(0, mhas.size());
    }


    @Test
    public void testGetDalClusterNode() throws JsonProcessingException {
        String response = "{\n" +
                "    \"status\": 403,\n" +
                "    \"message\": \"REQUEST FORBIDDEN\",\n" +
                "    \"result\": \"Ip address is not in the whitelist, ip = xxxx\"\n" +
                "}";
        Mockito.doReturn(objectMapper.readTree(response).get("result")).when(dbClusterApiService).getDalClusterNode(Mockito.any());
        JsonNode result = dbClusterRetriever.getDalClusterNode();
        Assert.assertNotNull(result);
    }


    @Test
    public void testIgnoreTableConfigs() throws JsonProcessingException {

        String tableConfigResult = "{\n" +
                "    \"status\": 200,\n" +
                "    \"message\": \"query table configs success.\",\n" +
                "    \"result\": {\n" +
                "        \"defaultUcsShardColumn\": null,\n" +
                "        \"tableConfigs\": [\n" +
                "            {\n" +
                "                \"tableName\": \"tbl_a\",\n" +
                "                \"ucsShardColumn\": \"a_column\",\n" +
                "                \"ignoreReplication\": true\n" +
                "            },\n" +
                "            {\n" +
                "                \"tableName\": \"tbl_b\",\n" +
                "                \"ucsShardColumn\": \"b_column\",\n" +
                "                \"ignoreReplication\": true\n" +
                "            },\n" +
                "            {\n" +
                "                \"tableName\": \"tbl_c\",\n" +
                "                \"ucsShardColumn\": \"\",\n" +
                "                \"ignoreReplication\": true\n" +
                "            },\n" +
                "            {\n" +
                "                \"tableName\": \"tbl_d\",\n" +
                "                \"ucsShardColumn\": \"\",\n" +
                "                \"ignoreReplication\": true\n" +
                "            }\n" +
                "        ]\n" +
                "    }\n" +
                "}";
        Mockito.doReturn(objectMapper.readTree(tableConfigResult).get("result")).when(dbClusterApiService).getIgnoreTableConfigs(Mockito.any(),Mockito.eq("testClusterName"));
        List<TableConfig> tableConfigList = dbClusterRetriever.getIgnoreTableConfigs("testClusterName");
        Assert.assertTrue(tableConfigList.size() == 4);

        Set<String> names = Sets.newHashSet();
        for (TableConfig tableConfig : tableConfigList) {
            String tableName = tableConfig.getTableName();
            names.add(tableName);
        }

        Assert.assertTrue(names.contains("tbl_a"));
        Assert.assertTrue(names.contains("tbl_b"));
        Assert.assertTrue(names.contains("tbl_c"));
        Assert.assertTrue(names.contains("tbl_d"));
        

    }

}
