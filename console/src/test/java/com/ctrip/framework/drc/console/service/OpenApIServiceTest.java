package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.framework.drc.service.console.DbClusterApiServiceImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;

/**
 * Created by dengquanliang
 * 2023/6/6 15:40
 */
public class OpenApIServiceTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private OPSApiService opsApiServiceImpl = ApiContainer.getOPSApiServiceImpl();
    private DomainConfig domainConfig = new DomainConfig();
    private DalServiceImpl dalService = new DalServiceImpl();
    private DbClusterApiServiceImpl dbClusterApiService = new DbClusterApiServiceImpl();

    private static final ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false);
    private static final String FAT_URL = "http://service.dbcluster.fws.qa.nt.ctripcorp.com/api/dal/v2/";
    private static final String PRO_URL = "http://service.dbcluster.ctripcorp.com/api/dal/v2/";
    private static final String MHA_INSTANCES_GROUP_CLUSTER_INFO = "instanceGroups/%s/clusters?operator=DRCConsole";
    private static final List<String> FAT_MHA_LIST = Arrays.asList("sinfatpub01", "sin-drc2test", "commonordershardnt", "fat-fx-drc1");
    private static final List<String> PRO_MHA_LIST = Arrays.asList("sindrcpub03test", "sinfltpub01", "gscrawlerbigdatanew", "pkgproductmetanew");
    private static final String TOKEN = "6kx7Lihuc3vBNKeMWMa51gMgDbJkn9s4";
    public static final int DEFAULT_TIME_OUT = 10000;

    @Test
    public void testGetInstanceGroupsInfo() {
//        JsonNode jsonNode = dbClusterApiService.getInstanceGroupsInfo(PRO_URL, PRO_MHA_LIST);
        JsonNode jsonNode = dbClusterApiService.getInstanceGroupsInfo(FAT_URL, FAT_MHA_LIST);

        Map<String, Map<String, List<String>>> mhaDbNames = new HashMap<>();
        try {
            for(JsonNode instanceNode : jsonNode) {
                String mhaName = instanceNode.get("mhaName").asText();
                Map<String, List<String>> dalClusterDbs = new HashMap<>();

                JsonNode clusters = instanceNode.get("clusters");
                for(JsonNode cluster : clusters) {
                    String dalClusterName = cluster.get("clusterName").asText();
                    List<String> dbNames = Lists.newArrayList();
                    JsonNode shards = cluster.get("shards");
                    for (JsonNode shard : shards) {
                        dbNames.add(shard.get("dbName").asText());
                    }
                    if (!dbNames.isEmpty()) {
                        dalClusterDbs.put(dalClusterName, dbNames);
                    }
                }

                mhaDbNames.put(mhaName, dalClusterDbs);
            }
        } catch (Exception e) {
            logger.error("Fail generate getDbNames, ", e);
        }

        mhaDbNames.forEach((k, v) -> {
            System.out.print(k + ": ");
            System.out.println(v);
        });
    }

    @Test
    public void testGetInstanceGroupsInfos() throws Exception {
        String url = String.format(PRO_URL + MHA_INSTANCES_GROUP_CLUSTER_INFO, StringUtils.join(PRO_MHA_LIST, ","));
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .header("X-Access-Token", TOKEN)
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(DEFAULT_TIME_OUT))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JsonNode jsonNode = objectMapper.readTree(response.body()).get("result");
//        for(JsonNode instanceNode : jsonNode) {
//            System.out.println(instanceNode.get("mhaName").asText());
//        }

        Map<String, Map<String, List<String>>> mhaDbNames = new HashMap<>();
        try {
            for(JsonNode instanceNode : jsonNode) {
                String mhaName = instanceNode.get("mhaName").asText();
                Map<String, List<String>> dalClusterDbs = new HashMap<>();

                JsonNode clusters = instanceNode.get("clusters");
                for(JsonNode cluster : clusters) {
                    String dalClusterName = cluster.get("clusterName").asText();
                    List<String> dbNames = Lists.newArrayList();
                    JsonNode shards = cluster.get("shards");
                    for (JsonNode shard : shards) {
                        dbNames.add(shard.get("dbName").asText());
                    }
                    if (!dbNames.isEmpty()) {
                        dalClusterDbs.put(dalClusterName, dbNames);
                    }
                }

                mhaDbNames.put(mhaName, dalClusterDbs);
            }
        } catch (Exception e) {
            logger.error("Fail generate getDbNames, ", e);
        }

        mhaDbNames.forEach((k, v) -> {
            System.out.print(k + ": ");
            System.out.println(v);
        });
    }

    @Test
    public void testGetAllClusterInfo() throws JsonProcessingException {
        String getAllClusterUrl = domainConfig.getGetAllClusterUrl();
        String opsAccessToken = domainConfig.getOpsAccessToken();
        System.out.println("url: " + getAllClusterUrl);
        System.out.println("toke: " + opsAccessToken);
        JsonNode jsonNode = opsApiServiceImpl.getAllClusterInfo(getAllClusterUrl, opsAccessToken);
        System.out.println("jsonNode: " + jsonNode);
    }

    @Test
    public void testGetAllClusterNames() {
        List<Map<String, String>> res = new ArrayList<>();
        try {
            JsonNode root = getAllClusterInfo();
            for(JsonNode cluster : root.get("data")){
                Map<String,String> clusterPair = new HashMap<>();
                clusterPair.put("cluster", cluster.get("cluster").asText());
                JsonNode machines = cluster.get("machines");
                if(machines.isArray()){
                    for(JsonNode machine:machines){
                        if("master".equalsIgnoreCase(machine.get("role").asText())) {
                            clusterPair.put("zoneId", machine.get("machine_located").asText());
                            break;
                        }
                    }
                }
                res.add(clusterPair);
            }
        } catch (JsonProcessingException e) {
            logger.error("JsonProcessingException", e);
        }
        System.out.println(res.size());
        System.out.println(res);
    }

    private JsonNode getAllClusterInfo() throws JsonProcessingException {
        String getAllClusterUrl = domainConfig.getGetAllClusterUrl();
        String opsAccessToken = domainConfig.getOpsAccessToken();
        System.out.println("url: " + getAllClusterUrl);
        System.out.println("toke: " + opsAccessToken);
        JsonNode jsonNode = opsApiServiceImpl.getAllClusterInfo(getAllClusterUrl, opsAccessToken);

        return jsonNode;
    }
}
