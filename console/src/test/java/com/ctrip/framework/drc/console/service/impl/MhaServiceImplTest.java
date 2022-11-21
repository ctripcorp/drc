package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.ClusterMhaMapTbl;
import com.ctrip.framework.drc.console.dao.entity.ClusterTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.dto.DalClusterDto;
import com.ctrip.framework.drc.console.dto.DalClusterShard;
import com.ctrip.framework.drc.console.dto.DalMhaDto;
import com.ctrip.framework.drc.console.dto.MhaDto;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.codec.JsonCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;

/**
 * @author maojiawei
 * @version 1.0
 * date: 2020-08-06
 */

public class MhaServiceImplTest {

    @InjectMocks private MhaServiceImpl mhaService;

    @Mock private DefaultConsoleConfig defaultConsoleConfig;

    @Mock private MonitorTableSourceProvider monitorTableSourceProvider;

    @Mock private OPSApiService opsApiServiceImpl;
    
    @Mock private DbClusterApiService dbClusterApiServiceImpl;

    @Mock private DomainConfig domainConfig;

    @Mock private MhaTblDao mhaTblDao;

    @Mock private ClusterMhaMapTblDao clusterMhaMapTblDao;

    @Mock private ClusterTblDao clusterTblDao;

    @Mock private BuTblDao buTblDao;

    @Mock private DcTblDao dcTblDao;

    @Mock private DalUtils dalUtils;


    private ObjectMapper objectMapper = new ObjectMapper();

    private JsonNode shard;

    private JsonNode root;


    public static Map<String, String> getDbaDcInfoMapping(String dbaDcInfoStr) {
        Map<String, String> dbaDcInfos = JsonCodec.INSTANCE.decode(dbaDcInfoStr, new GenericTypeReference<Map<String, String>>() {});

        Map<String, String> result = Maps.newConcurrentMap();
        for(Map.Entry<String, String> entry : dbaDcInfos.entrySet()){
            result.put(entry.getKey(), entry.getValue().toLowerCase());
        }
        return result;
    }
    
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        doReturn("off").when(monitorTableSourceProvider).getCacheAllClusterNamesSwitch();
        doReturn(getDbaDcInfoMapping("{\"上海欧阳IDC(电信)\":\"shaoy\", \"上海日阪IDC(联通)\":\"sharb\", \"上海金钟路B栋\":\"shajz\", \"上海金桥IDC(联通)\":\"shajq\", \"上海福泉路\":\"shafq\", \"南通星湖大道\":\"ntgxh\", \"上海SOHO大楼\":\"shash\"}")).when(defaultConsoleConfig).getDbaDcInfos();
        try {
            root = objectMapper.readTree("{\"status\":200,\"message\":\"query cluster success.\",\"result\":{\"clusterName\":\"bbzdrccameldb_dalcluster\",\"type\":\"normal\",\"unitStrategyId\":null,\"unitStrategyName\":null,\"shardStrategies\":null,\"idGenerators\":null,\"dbCategory\":\"mysql\",\"enabled\":true,\"released\":true,\"releaseVersion\":97,\"createTime\":null,\"lastReleaseTime\":null,\"shards\":[{\"shardIndex\":0,\"dbName\":\"bbzdrccameldb\",\"zones\":[{\"zoneId\":\"ntgxh\",\"active\":true,\"master\":{\"domain\":\"bbzdrccamel.mysql.db.fat.ntgxh.qa.nt.ctripcorp.com\",\"port\":55111,\"instance\":{\"ip\":\"10.2.72.247\",\"port\":55111,\"tags\":null},\"instances\":null},\"slave\":{\"domain\":null,\"port\":null,\"instance\":null,\"instances\":[{\"ip\":\"10.2.72.230\",\"port\":55111,\"tags\":null}]},\"read\":null}],\"users\":[{\"username\":\"t_bbzdrccamel\",\"password\":null,\"permission\":\"write\",\"tag\":\"\",\"enabled\":true,\"titanKey\":null}]}]}}");
            shard = objectMapper.readTree("{\"shardIndex\":1,\"dbName\":\"corpcostsetshard01db\",\"zones\":[{\"zoneId\":\"ntgxh\",\"active\":true,\"master\":{\"domain\":\"corpcostsetshard01.mysql.db.fat.qa.nt.ctripcorp.com\",\"port\":55111,\"instance\":{\"ip\":\"10.5.20.230\",\"port\":55111,\"tags\":null},\"instances\":null},\"slave\":{\"domain\":null,\"port\":null,\"instance\":null,\"instances\":[{\"ip\":\"10.5.20.229\",\"port\":55111,\"tags\":null}]},\"read\":null}],\"users\":[{\"username\":\"tt_corpset_1_2\",\"password\":null,\"permission\":\"write\",\"tag\":\"\",\"enabled\":true,\"titanKey\":\"corpcostsetshard01db_w\"}]}");
        } catch (IOException e) {
        }
    }

    @Test
    public void testGetCachedAllClusterNames() throws Exception {
        String response = "{\"message\":\"success\"," +
                "\"data\":[" +
                "{\"db_type\":\"MySQL\"," +
                "\"cluster\":\"cluster1\"," +
                "\"db_list\":[{\"has_dr\":1,\"organization_id\":51,\"db_name\":\"pkgMarketingDB\",\"productline_name\":null,\"organization_name\":\"dujia\",\"product_id\":null,\"level\":1,\"db_name_base\":\"pkgMarketingDB\",\"productline_id\":null,\"product_name\":null,\"dbowners\":\"wping;hltu\",\"db_createtime\":\"2018-09-1015:32:43\"},{\"has_dr\":1,\"organization_id\":51,\"db_name\":\"pkgmarketplatformdb\",\"productline_name\":null,\"organization_name\":\"dujia\",\"product_id\":null,\"level\":1,\"db_name_base\":\"pkgmarketplatformdb\",\"productline_id\":null,\"product_name\":null,\"dbowners\":\"yangk\",\"db_createtime\":\"2018-12-0415:53:32\"},{\"has_dr\":1,\"organization_id\":51,\"db_name\":\"pkgprivilegedb\",\"productline_name\":null,\"organization_name\":\"dujia\",\"product_id\":null,\"level\":1,\"db_name_base\":\"pkgprivilegedb\",\"productline_id\":null,\"product_name\":null,\"dbowners\":\"lurm\",\"db_createtime\":\"2018-09-1015:32:43\"}],\"type\":\"mha\",\"port\":55944,\"machines\":[{\"instance_port\":55944,\"machine_name\":\"SVR32066HC4700\",\"machine_located\":\"上海日阪IDC(联通)\",\"ci_code\":\"SVR32066HC4700\",\"role\":\"slave\",\"ip_business\":\"10.60.240.204\"},{\"instance_port\":55944,\"machine_name\":\"SVR32067HC4700\",\"machine_located\":\"上海日阪IDC(联通)\",\"ci_code\":\"SVR32067HC4700\",\"role\":\"master\",\"ip_business\":\"10.60.240.197\"},{\"instance_port\":55944,\"machine_name\":\"SVR35157HC4700\",\"machine_located\":\"上海新源IDC(移动)\",\"ci_code\":\"SVR35157HC4700\",\"role\":\"slave-dr\",\"ip_business\":\"10.109.196.13\"}]}],\"success\":true}";

        JsonNode jsonNode = objectMapper.readTree(response);
       

        Mockito.doReturn(jsonNode).when(opsApiServiceImpl).getAllClusterInfo(Mockito.any(),Mockito.any());
        List<Map<String, String>> clusterNames = mhaService.getCachedAllClusterNames();
        Assert.assertNotNull(clusterNames);
        Assert.assertNotEquals(0, clusterNames.size());

        clusterNames = mhaService.getCachedAllClusterNames("");
        Assert.assertNotNull(clusterNames);
        Assert.assertNotEquals(0, clusterNames.size());

        clusterNames = mhaService.getCachedAllClusterNames("drc");
        Assert.assertEquals(0, clusterNames.size());

        clusterNames = mhaService.getCachedAllClusterNames("nosuchmha");
        Assert.assertNotNull(clusterNames);
        Assert.assertEquals(0, clusterNames.size());


    }
    

    @Test
    public void testGetAllClusterNames() throws Exception {
        String response ="{\"message\":\"success\"," +
                "\"data\":[" +
                "{\"db_type\":\"MySQL\"," +
                "\"cluster\":\"cluster1\"," +
                "\"db_list\":[" +
                "{\"has_dr\":1," +
                "\"organization_id\":51," +
                "\"db_name\":\"pkgMarketingDB\"," +
                "\"productline_name\":null," +
                "\"organization_name\":\"dujia\"," +
                "\"product_id\":null,\"level\":1," +
                "\"db_name_base\":\"pkgMarketingDB\"," +
                "\"productline_id\":null,\"product_name\":null," +
                "\"dbowners\":\"wping;hltu\"," +
                "\"db_createtime\":\"2018-09-1015:32:43\"}," +
                "{\"has_dr\":1," +
                "\"organization_id\":51," +
                "\"db_name\":\"pkgmarketplatformdb\"," +
                "\"productline_name\":null," +
                "\"organization_name\":\"dujia\"," +
                "\"product_id\":null,\"level\":1," +
                "\"db_name_base\":\"pkgmarketplatformdb\"," +
                "\"productline_id\":null,\"product_name\":null," +
                "\"dbowners\":\"yangk\"," +
                "\"db_createtime\":\"2018-12-0415:53:32\"}," +
                "{\"has_dr\":1,\"organization_id\":51," +
                "\"db_name\":\"pkgprivilegedb\",\"productline_name\":null," +
                "\"organization_name\":\"dujia\",\"product_id\":null,\"level\":1," +
                "\"db_name_base\":\"pkgprivilegedb\",\"productline_id\":null," +
                "\"product_name\":null,\"dbowners\":\"lurm\"," +
                "\"db_createtime\":\"2018-09-1015:32:43\"}]," +
                "\"type\":\"mha\"," +
                "\"port\":55944," +
                "\"machines\":[" +
                "{\"instance_port\":55944,\"machine_name\":\"4700\",\"machine_located\":\"上海日阪IDC(联通)\",\"ci_code\":\"SVR32066HC4700\",\"role\":\"slave\",\"ip_business\":\"ip1\"}," +
                "{\"instance_port\":55944,\"machine_name\":\"4700\",\"machine_located\":\"上海日阪IDC(联通)\",\"ci_code\":\"SVR32067HC4700\",\"role\":\"master\",\"ip_business\":\"ip2\"}," +
                "{\"instance_port\":55944,\"machine_name\":\"4700\",\"machine_located\":\"上海新源IDC(移动)\",\"ci_code\":\"SVR35157HC4700\",\"role\":\"slave-dr\",\"ip_business\":\"ip3\"}]}]," +
                "\"success\":true}";
        JsonNode jsonNode = objectMapper.readTree(response);

        Mockito.doReturn(jsonNode).when(opsApiServiceImpl).getAllClusterInfo(Mockito.any(),Mockito.any());
        List<Map<String, String>> clusterNames = mhaService.getAllClusterNames();
        Assert.assertEquals(1, clusterNames.size());

    }
    
    @Test 
    public void testGetMasterMachineInstance() throws Exception {
        String response ="{\"message\":\"success\"," +
                "\"data\":[" +
                "{\"db_type\":\"MySQL\"," +
                "\"cluster\":\"cluster1\"," +
                "\"db_list\":[" +
                "{\"has_dr\":1," +
                "\"organization_id\":51," +
                "\"db_name\":\"pkgMarketingDB\"," +
                "\"productline_name\":null," +
                "\"organization_name\":\"dujia\"," +
                "\"product_id\":null,\"level\":1," +
                "\"db_name_base\":\"pkgMarketingDB\"," +
                "\"productline_id\":null,\"product_name\":null," +
                "\"dbowners\":\"wping;hltu\"," +
                "\"db_createtime\":\"2018-09-1015:32:43\"}," +
                "{\"has_dr\":1," +
                "\"organization_id\":51," +
                "\"db_name\":\"pkgmarketplatformdb\"," +
                "\"productline_name\":null," +
                "\"organization_name\":\"dujia\"," +
                "\"product_id\":null,\"level\":1," +
                "\"db_name_base\":\"pkgmarketplatformdb\"," +
                "\"productline_id\":null,\"product_name\":null," +
                "\"dbowners\":\"yangk\"," +
                "\"db_createtime\":\"2018-12-0415:53:32\"}," +
                "{\"has_dr\":1,\"organization_id\":51," +
                "\"db_name\":\"pkgprivilegedb\",\"productline_name\":null," +
                "\"organization_name\":\"dujia\",\"product_id\":null,\"level\":1," +
                "\"db_name_base\":\"pkgprivilegedb\",\"productline_id\":null," +
                "\"product_name\":null,\"dbowners\":\"lurm\"," +
                "\"db_createtime\":\"2018-09-1015:32:43\"}]," +
                "\"type\":\"mha\"," +
                "\"port\":55944," +
                "\"machines\":[" +
                "{\"instance_port\":55944,\"machine_name\":\"4700\",\"machine_located\":\"上海日阪IDC(联通)\",\"ci_code\":\"SVR32066HC4700\",\"role\":\"slave\",\"ip_business\":\"ip1\"}," +
                "{\"instance_port\":55944,\"machine_name\":\"4700\",\"machine_located\":\"上海日阪IDC(联通)\",\"ci_code\":\"SVR32067HC4700\",\"role\":\"master\",\"ip_business\":\"ip2\"}," +
                "{\"instance_port\":55944,\"machine_name\":\"4700\",\"machine_located\":\"上海新源IDC(移动)\",\"ci_code\":\"SVR35157HC4700\",\"role\":\"slave-dr\",\"ip_business\":\"ip3\"}]}]," +
                "\"success\":true}";
        JsonNode jsonNode = objectMapper.readTree(response);

        Mockito.doReturn(jsonNode).when(opsApiServiceImpl).getAllClusterInfo(Mockito.any(),Mockito.any());
        Endpoint endpoint = mhaService.getMasterMachineInstance("cluster1");
        Assert.assertEquals("ip2", endpoint.getHost());
        endpoint = mhaService.getMasterMachineInstance("nosuchmha");
        Assert.assertNull(endpoint);

        String dc = mhaService.getDcForMha("cluster1");
        System.out.println(dc);

    }

    @Test
    public void testGetGetAllDbs() throws Exception {
        String reponse = "{\"message\": \"ok\", \"data\": [{\"cluster_name\": \"fat-fx-drc1\", \"env_type\": \"fat\", \"db_name\": \"bbzbbzdrcbenchmarktmpdb\"},\n" +
                "{\"cluster_name\": \"fat-fx-drc1\", \"env_type\": \"fat\", \"db_name\": \"bbzdrcbenchmarkdb\"}, {\"cluster_name\": \"fat-fx-drc1\",\n" +
                "\"env_type\": \"fat\", \"db_name\": \"bbzdrccameldb\"}], \"success\": true}";

        Mockito.doReturn(objectMapper.readTree(reponse)).when(opsApiServiceImpl).getAllDbs(Mockito.any(),Mockito.any(),eq("fat-fx-drc1"), eq("fat"));
        List<String> dbNames = mhaService.getAllDbs("fat-fx-drc1", "fat");
        System.out.println(dbNames);
        Assert.assertNotNull(dbNames);
        Assert.assertNotEquals(0, dbNames.size());

    }
    
    @Test
    public void testGetAllDbsAndDals(){
        Map<String, Object> dalClusterRetMap = new HashMap<>();

        List<DalClusterDto> clusterList = new ArrayList<>();
        List<DalClusterShard> noShard1 = new ArrayList<>();
        DalClusterShard dalClusterNoShard = new DalClusterShard(0, "singleDb1", "oy");
        noShard1.add(dalClusterNoShard);
        DalClusterDto dalClusterDtoNoShard = new DalClusterDto("noShardClusterName1", "singleDb1", noShard1);
        clusterList.add(dalClusterDtoNoShard);

        List<DalClusterShard> Shard1 = new ArrayList<>();
        DalClusterShard dalClusterShardDb1 = new DalClusterShard(0, "shardDb1", "oy");
        DalClusterShard dalClusterShardDb2 = new DalClusterShard(1, "shardDb2", "oy");
        Shard1.add(dalClusterShardDb1);
        Shard1.add(dalClusterShardDb2);
        DalClusterDto dalClusterDtoShard = new DalClusterDto("ShardClusterName1", "shardDb", Shard1);
        clusterList.add(dalClusterDtoShard);

        DalMhaDto dalMhaDto = new DalMhaDto("testMhaName", clusterList);
        List<DalMhaDto> dalMhaDtoList = new ArrayList<>();
        dalMhaDtoList.add(dalMhaDto);
        dalClusterRetMap.put("result", dalMhaDtoList);

        Mockito.doReturn(dalClusterRetMap).when(dbClusterApiServiceImpl).getDalClusterFromDalService(Mockito.any(),eq("testMhaName"));
        mhaService.getAllDbsAndDals("testMhaName", "fat", "oy");

    }


    @Test
    public void testRecordMha() throws SQLException {
        MhaDto mockDto = new MhaDto();
        mockDto.setMhaName("mha1");
        mockDto.setDc("dc1");
        mockDto.setBuName("bu1");
        mockDto.setAppid(1L);
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.anyString(),Mockito.anyInt())).thenReturn(new MhaTbl());
        ApiResult apiResult = mhaService.recordMha(mockDto);
        Assert.assertEquals("mha already exist!",apiResult.getMessage());

        Mockito.when(mhaTblDao.queryByMhaName(Mockito.anyString(),Mockito.anyInt())).thenReturn(null);
        Mockito.when(dalUtils.updateOrCreateDc(Mockito.anyString())).thenReturn(1L);
        Mockito.when(dalUtils.updateOrCreateBu(Mockito.anyString())).thenReturn(1L);
        Mockito.when(dalUtils.updateOrCreateCluster(Mockito.anyString(),Mockito.anyLong(),Mockito.anyLong())).thenReturn(1L);
        Mockito.when(dalUtils.updateOrCreateMha(Mockito.anyString(),Mockito.anyLong())).thenReturn(1L);
        Mockito.when(dalUtils.updateOrCreateClusterMhaMap(Mockito.anyLong(),Mockito.anyLong())).thenReturn(1L);

        apiResult = mhaService.recordMha(mockDto);
        Assert.assertEquals(0,apiResult.getStatus().intValue());
        
    }

    @Test
    public void testQueryMhaInfo() throws SQLException {
        MhaTbl mockMhaTbl = new MhaTbl();
        mockMhaTbl.setMhaName("mha1");
        mockMhaTbl.setMonitorSwitch(1);
        ClusterMhaMapTbl mockClusterMhaMapTbl = new ClusterMhaMapTbl();
        mockClusterMhaMapTbl.setClusterId(1L);
        mockClusterMhaMapTbl.setMhaId(1L);
        ClusterTbl mockClusterTbl = new ClusterTbl();
        mockClusterTbl.setBuId(1L);
        BuTbl mockBuTbl = new BuTbl();
        mockBuTbl.setBuName("bu1");
        
        Mockito.when(mhaTblDao.queryByPk(Mockito.anyLong())).thenReturn(mockMhaTbl);
        Mockito.when(clusterMhaMapTblDao.queryByMhaIds(Mockito.anyList(),Mockito.anyInt())).
                thenReturn(Lists.newArrayList(mockClusterMhaMapTbl));
        Mockito.when(clusterTblDao.queryByPk(Mockito.anyLong())).thenReturn(mockClusterTbl);
        Mockito.when(buTblDao.queryByPk(Mockito.anyLong())).thenReturn(mockBuTbl);
        MhaDto mhaDto = mhaService.queryMhaInfo(1L);
        Assert.assertEquals("mha1",mhaDto.getMhaName());

    }

    
}
