package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.AllTests;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dto.RouteDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.EstablishStatusEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.vo.MhaGroupPair;
import com.ctrip.framework.drc.console.vo.MhaGroupPairVo;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import org.assertj.core.util.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.AllTests.DRC_XML;
import static com.ctrip.framework.drc.console.AllTests.DRC_XML_one2many;
import static com.ctrip.framework.drc.console.service.impl.MetaGeneratorTest.*;


public class MetaInfoServiceImplTest extends AbstractTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    public static final String MHA1OY = "fat-fx-drc1";

    public static final String MHA1RB1 = "fat-fx-drc2";
    public static final String MHA1RB2 = "fat-fx-drc3";

    public static final String MHA2OY = "drcTestW1";

    public static final String MHA2RB = "drcTestW2";

    public static final String DAL_CLUSTER = "integration-test";
    public static final String DAL_CLUSTER2 = "dalcluster2";
    public static final String DAL_CLUSTER3 = "dalcluster3";

    private List<String> mhas = Arrays.asList("fat-fx-drc1", "fat-fx-drc2");

    @InjectMocks
    private MetaInfoServiceImpl metaInfoService;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Mock
    private MetaGenerator metaService;

    @Mock
    private DalServiceImpl dalService;

    @Mock
    private DefaultConsoleConfig defaultConsoleConfig;

    @Mock
    private DbClusterSourceProvider dbClusterSourceProvider;

    private DalUtils dalUtils = DalUtils.getInstance();

    private Long mhaGroupId = 1L;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        init();
    }

    @Test
    public void testGetMhaTbls() throws SQLException {
        List<MhaTbl> mhaTbls = metaInfoService.getMhaTbls(mhaGroupId);
        Assert.assertEquals(2, mhaTbls.size());
        List<String> mhaNames = Arrays.asList(MHA1OY, MHA1RB1);
        for(MhaTbl mhaTbl : mhaTbls) {
            Assert.assertTrue(mhaNames.contains(mhaTbl.getMhaName()));
        }
    }

    /**
     * getMhaGroupId for given mhaNames or mhaName
     */
//    @Test
//    public void testGetMhaGroupId() throws SQLException {
//        Set<String> mhaNames = Sets.newHashSet(Arrays.asList("nosuchmha"));
//        Assert.assertNull(metaInfoService.getMhaGroupId(mhaNames));
//
//        mhaNames = Sets.newHashSet(Arrays.asList(MHA1OY, MHA1RB));
//        Assert.assertEquals(mhaGroupId, metaInfoService.getMhaGroupId(mhaNames));
//
//        mhaNames = Sets.newHashSet(Arrays.asList(MHA2OY, MHA2RB));
//        Assert.assertNull(metaInfoService.getMhaGroupId(mhaNames));
//    }

    @Test
    public void testGetMhaGroupForMha() throws Exception {

        MhaGroupTbl nosuchmha = metaInfoService.getMhaGroupForMha("nosuchmha");
        Assert.assertNull(nosuchmha);

        MhaGroupTbl mhaGroupTbl = metaInfoService.getMhaGroupForMha(MHA1OY);
        Assert.assertNotNull(mhaGroupTbl);
        Assert.assertEquals(mhaGroupId, mhaGroupTbl.getId());
    }

    @Test
    public void testGetReplicatorGroupId() {
        Assert.assertEquals(1L, metaInfoService.getReplicatorGroupId(MHA1OY).longValue());
        Assert.assertEquals(2L, metaInfoService.getReplicatorGroupId(MHA1RB1).longValue());
    }

    @Test
    public void testGetMachines2() throws SQLException {
        Long mhaGroupId = metaInfoService.getMhaGroupId("fat-fx-drc1");
        List<DBInfo> machines = metaInfoService.getMachines(mhaGroupId);
        Assert.assertNotEquals(0, machines.size());
    }

    @Test
    public void testGetMhaTblMap() throws SQLException {
        Long mhaGroupId = metaInfoService.getMhaGroupId("fat-fx-drc1");
        Map<String, MhaTbl> mhaTblMap = metaInfoService.getMhaTblMap(mhaGroupId);
        Assert.assertNotEquals(0, mhaTblMap.size());
    }

    @Test
    public void testGetAllMhaNamesInCluster() throws Exception {

        ClusterTbl clusterTbl = dalUtils.getClusterTblDao().queryAll().stream().filter(p -> (p.getDeleted().equals(BooleanEnum.FALSE.getCode()) && "integration-test".equalsIgnoreCase(p.getClusterName()))).findFirst().orElse(null);
        Assert.assertNotNull(clusterTbl);
        List<String> mhaNames = metaInfoService.getAllMhaNamesInCluster(clusterTbl.getId());
        Assert.assertNotNull(mhaNames);
        System.out.println(mhaNames);
        List<String> expectedMhas = new ArrayList<>() {{
            add(MHA1OY);
            add(MHA1RB1);
            add(MHA1RB2);
        }};
        Assert.assertTrue(AllTests.weakEquals(expectedMhas, mhaNames));
    }

    @Test
    public void testGetMachines() throws Exception {
        List<String> machines = metaInfoService.getMachines("fat-fx-drc1");
        Assert.assertEquals(2, machines.size());
        Assert.assertEquals("10.2.72.230:55111", machines.get(0));

        machines = metaInfoService.getMachines("fat-fx-drc2");
        Assert.assertEquals(2, machines.size());
        Assert.assertEquals("10.2.72.246:55111", machines.get(0));
    }

    @Test
    public void testGetAllMhaGroups() throws Exception {
        List<MhaGroupPair> allMhaGroups = metaInfoService.getAllMhaGroups();
        Assert.assertEquals(2, allMhaGroups.size());
        MhaGroupPair mhaGroupPair = allMhaGroups.get(0);
        Assert.assertEquals(MHA1OY, mhaGroupPair.getSrcMha());
        Assert.assertEquals(MHA1RB1, mhaGroupPair.getDestMha());
        Assert.assertEquals(EstablishStatusEnum.ESTABLISHED.getCode(), mhaGroupPair.getDrcEstablishStatus().intValue());
        mhaGroupPair = allMhaGroups.get(1);
        Assert.assertEquals(MHA1OY, mhaGroupPair.getSrcMha());
        Assert.assertEquals(MHA1RB2, mhaGroupPair.getDestMha());
        Assert.assertEquals(EstablishStatusEnum.ESTABLISHED.getCode(), mhaGroupPair.getDrcEstablishStatus().intValue());
    }

    @Test
    public void testGetAllOrderedGroupPairs() throws Exception {
        List<MhaGroupPairVo> allMhaGroups = metaInfoService.getAllOrderedGroupPairs();
        Assert.assertEquals(2, allMhaGroups.size());
        MhaGroupPairVo mhaGroupPairVo = allMhaGroups.get(0);
        Assert.assertEquals(MHA1OY, mhaGroupPairVo.getSrcMha());
        Assert.assertEquals(MHA1RB1, mhaGroupPairVo.getDestMha());
        Assert.assertEquals(EstablishStatusEnum.ESTABLISHED.getCode(), mhaGroupPairVo.getDrcEstablishStatus().intValue());
        mhaGroupPairVo = allMhaGroups.get(1);
        Assert.assertEquals(MHA1OY, mhaGroupPairVo.getSrcMha());
        Assert.assertEquals(MHA1RB2, mhaGroupPairVo.getDestMha());
        Assert.assertEquals(EstablishStatusEnum.ESTABLISHED.getCode(), mhaGroupPairVo.getDrcEstablishStatus().intValue());
    }


    @Test
    public void testGetXmlConfiguration() throws Exception {
        String s = metaInfoService.getXmlConfiguration(MHA1OY, MHA1RB1);
        System.out.println(s);
        Drc drc = DefaultSaxParser.parse(s);
        Assert.assertEquals(2, drc.getDcs().size());

        s = metaInfoService.getXmlConfiguration(MHA1OY, MHA1RB2);
        System.out.println(s);
        drc = DefaultSaxParser.parse(s);
        Assert.assertEquals(2, drc.getDcs().size());
    }

    @Test
    public void testGetResourcesMethods() throws Exception {

        List<String> r = metaInfoService.getResourcesInDcOfMha("fat-fx-drc1", "R");
        System.out.println("r in dc: " + r);
        Assert.assertEquals(3, r.size());

        List<String> a = metaInfoService.getResourcesInDcOfMha("fat-fx-drc1", "A");
        System.out.println("a in dc: " + a);
        Assert.assertEquals(3, a.size());

        r = metaInfoService.getResourcesInUse("fat-fx-drc1", "", "R");
        System.out.println("r in use: " + r);
        Assert.assertEquals(2, r.size());

        a = metaInfoService.getResourcesInUse("fat-fx-drc1", "fat-fx-drc2", "A");
        System.out.println("a in use: " + a);
        Assert.assertEquals(2, a.size());
    }

    @Test
    public void testGetIncludedDbs() throws SQLException {
        String includedDbs = metaInfoService.getIncludedDbs("fat-fx-drc1", "fat-fx-drc2");
        Assert.assertEquals("drcmonitordb", includedDbs);

        includedDbs = metaInfoService.getIncludedDbs("fat-fx-drc2", "fat-fx-drc1");
        Assert.assertNull(includedDbs);
    }
    
    @Test
    public void testGetTargetName() throws SQLException {
        String targetName = metaInfoService.getTargetName("fat-fx-drc1", "fat-fx-drc2");
        Assert.assertEquals("integration-test",targetName);
    }

    // take effect of DrcBuildServiceImplTest.testSubmitConfig
    @Test
    public void testGetReplicatorMethods() throws Exception {

        List<Integer> replicatorInstances1 = metaInfoService.getReplicatorInstances("10.2.83.105");
        List<Integer> replicatorInstances2 = metaInfoService.getReplicatorInstances("10.2.83.106");
        List<Integer> replicatorInstances3 = metaInfoService.getReplicatorInstances("10.2.86.199");
        List<Integer> replicatorInstances4 = metaInfoService.getReplicatorInstances("10.2.87.154");
        List<Integer> replicatorInstances5 = metaInfoService.getReplicatorInstances("10.2.87.153");

        System.out.println(replicatorInstances1);
        System.out.println(replicatorInstances2);
        System.out.println(replicatorInstances3);
        System.out.println(replicatorInstances4);
        System.out.println(replicatorInstances5);
        Assert.assertEquals(1, replicatorInstances1.size());
        Assert.assertEquals(2, replicatorInstances2.size());
        Assert.assertEquals(2, replicatorInstances3.size());
        Assert.assertEquals(1, replicatorInstances4.size());
        Assert.assertEquals(0, replicatorInstances5.size());
    }

    @Test
    public void testGetMasterEndpoint() throws SQLException {
        List<MhaTbl> mhaTbls = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted())).collect(Collectors.toList());
        Assert.assertEquals(3, mhaTbls.size());
        List<String> expectedMhas = new ArrayList<>() {{
            add(MHA1OY);
            add(MHA1RB1);
            add(MHA1RB2);
        }};

        for(MhaTbl mhaTbl : mhaTbls) {
            Assert.assertTrue(expectedMhas.contains(mhaTbl.getMhaName()));
            if(MHA1OY.equals(mhaTbl.getMhaName())) {
                Assert.assertEquals("10.2.72.230", metaInfoService.getMasterEndpoint(mhaTbl).getHost());
            } else if (MHA1RB1.equalsIgnoreCase(mhaTbl.getMhaName())) {
                Assert.assertEquals("10.2.72.246", metaInfoService.getMasterEndpoint(mhaTbl).getHost());
            } else {
                Assert.assertEquals("10.2.72.241", metaInfoService.getMasterEndpoint(mhaTbl).getHost());
            }
        }
    }

    @Test
    public void testFindAvailableApplierPort() throws SQLException {
        Assert.assertEquals(8384, metaInfoService.findAvailableApplierPort("10.2.83.105").intValue());
        Assert.assertEquals(8889, metaInfoService.findAvailableApplierPort("10.2.87.154").intValue());
    }

    private void init() throws SQLException {
        mock();
    }

    private void mock() throws SQLException {
        List<BuTbl> buTbls = dalUtils.getBuTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ClusterMhaMapTbl> clusterMhaMapTbls = dalUtils.getClusterMhaMapTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DcTbl> dcTbls = dalUtils.getDcTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ClusterTbl>  clusterTbls = dalUtils.getClusterTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaGroupTbl> mhaGroupTbls = dalUtils.getMhaGroupTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaTbl> mhaTbls = dalUtils.getMhaTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ResourceTbl> resourceTbls = dalUtils.getResourceTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MachineTbl> machineTbls = dalUtils.getMachineTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ReplicatorGroupTbl> replicatorGroupTbls = dalUtils.getReplicatorGroupTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierGroupTbl> applierGroupTbls = dalUtils.getApplierGroupTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ClusterManagerTbl> clusterManagerTbls = dalUtils.getClusterManagerTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ZookeeperTbl> zookeeperTbls = dalUtils.getZookeeperTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ReplicatorTbl> replicatorTbls = dalUtils.getReplicatorTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierTbl> applierTbls = dalUtils.getApplierTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Mockito.when(metaService.getBuTbls()).thenReturn(buTbls);
        Mockito.when(metaService.getClusterMhaMapTbls()).thenReturn(clusterMhaMapTbls);
        Mockito.when(metaService.getDcTbls()).thenReturn(dcTbls);
        Mockito.when(metaService.getClusterTbls()).thenReturn(clusterTbls);
        Mockito.when(metaService.getMhaGroupTbls()).thenReturn(mhaGroupTbls);
        Mockito.when(metaService.getMhaTbls()).thenReturn(mhaTbls);
        Mockito.when(metaService.getResourceTbls()).thenReturn(resourceTbls);
        Mockito.when(metaService.getMachineTbls()).thenReturn(machineTbls);
        Mockito.when(metaService.getReplicatorGroupTbls()).thenReturn(replicatorGroupTbls);
        Mockito.when(metaService.getApplierGroupTbls()).thenReturn(applierGroupTbls);
        Mockito.when(metaService.getClusterManagerTbls()).thenReturn(clusterManagerTbls);
        Mockito.when(metaService.getZookeeperTbls()).thenReturn(zookeeperTbls);
        Mockito.when(metaService.getReplicatorTbls()).thenReturn(replicatorTbls);
        Mockito.when(metaService.getApplierTbls()).thenReturn(applierTbls);
    }

    @Test
    public void testGetRealDalClusters() {
        Set<String> realDalClusters = metaInfoService.getRealDalClusters(DAL_CLUSTER, null);
        Assert.assertEquals(1, realDalClusters.size());

        realDalClusters = metaInfoService.getRealDalClusters(DAL_CLUSTER, Lists.newArrayList());
        Assert.assertEquals(1, realDalClusters.size());

        realDalClusters = metaInfoService.getRealDalClusters(DAL_CLUSTER, mhas);
        Map<String, String> mhaDalClusterInfo = new HashMap<>() {{
            put("fat-fx-drc1", "integration-test");
            put("fat-fx-drc2", "integration-test");
        }};
        Mockito.doReturn(mhaDalClusterInfo).when(dalService).getInstanceGroupsInfo(Mockito.anyList(), Mockito.any());
        realDalClusters = metaInfoService.getRealDalClusters(DAL_CLUSTER, mhas);
        Assert.assertEquals(1, realDalClusters.size());
        Assert.assertTrue(realDalClusters.contains(DAL_CLUSTER));
    }

    @Test
    public void testGetReplicator() throws SQLException {
        MhaTbl mhaTbl = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && "fat-fx-drc1".equalsIgnoreCase(p.getMhaName())).findFirst().orElse(null);
        InstanceInfo replicator = metaInfoService.getReplicator(mhaTbl);
        Assert.assertNotNull(replicator);
    }

    @Test
    public void testGetUcsStrategyIdMap() {
        Mockito.doReturn("qconfig").when(monitorTableSourceProvider).getUcsStrategyIdMapSource();
        Map<String, Integer> uidStrategyIds = new HashMap<>() {{
            put("testdb", 1);
            put("testdb2", 1);
        }};
        Mockito.doReturn(uidStrategyIds).when(defaultConsoleConfig).getUcsStrategyIdMap("integration-test");
        Map<String, Integer> ucsStrategyIdMap = metaInfoService.getUcsStrategyIdMap("integration-test", "fat-fx-drc1");
        Assert.assertEquals(2, ucsStrategyIdMap.size());
    }

    @Test
    public void testGetMasterMachine() {
        Endpoint masterMachine = metaInfoService.getMasterMachine("fat-fx-drc1");
        Assert.assertEquals("10.2.72.230", masterMachine.getHost());
    }

    @Test
    public void testGetRealDalClusterMap() {
        Map<String, List<String>> realDalClusterMap = metaInfoService.getRealDalClusterMap(DAL_CLUSTER, null);
        Assert.assertEquals(0, realDalClusterMap.size());
        realDalClusterMap = metaInfoService.getRealDalClusterMap(DAL_CLUSTER, Lists.newArrayList());
        Assert.assertEquals(0, realDalClusterMap.size());
        realDalClusterMap = metaInfoService.getRealDalClusterMap(DAL_CLUSTER, Arrays.asList("a", "b", "c"));
        Assert.assertEquals(0, realDalClusterMap.size());
        Map<String, String> mhaDalClusterInfo = new HashMap<>() {{
            put("fat-fx-drc1", "integration-test");
            put("fat-fx-drc2", "integration-test");
        }};
        Mockito.doReturn(mhaDalClusterInfo).when(dalService).getInstanceGroupsInfo(Mockito.anyList(), Mockito.any());
        realDalClusterMap = metaInfoService.getRealDalClusterMap(DAL_CLUSTER, mhas);
        Assert.assertEquals(1, realDalClusterMap.size());
        Assert.assertTrue(realDalClusterMap.containsKey(DAL_CLUSTER));
        Assert.assertEquals(2, realDalClusterMap.get(DAL_CLUSTER).size());
        Assert.assertTrue(realDalClusterMap.get(DAL_CLUSTER).contains(MHA1OY));
        Assert.assertTrue(realDalClusterMap.get(DAL_CLUSTER).contains(MHA1RB1));
    }

    @Test
    public void testBuildDalClusterMap() {
        Map<String, String> mhaDalClusterInfo = new HashMap<>() {{
            put("fat-fx-drc1", "integration-test,dalcluster2,dalcluster3");
            put("fat-fx-drc2", "integration-test,dalcluster2,dalcluster3");
            put("drcTestW1", "integration-test,dalcluster2");
            put("drcTestW2", "integration-test");
        }};
        Map<String, List<String>> roughDalClusterMap = metaInfoService.buildDalClusterMap(mhaDalClusterInfo);
        Assert.assertTrue(AllTests.weakEquals(Sets.newHashSet(Arrays.asList(DAL_CLUSTER, DAL_CLUSTER2, DAL_CLUSTER3)), roughDalClusterMap.keySet()));
        Assert.assertTrue(AllTests.weakEquals(Arrays.asList(MHA1OY, MHA1RB1, MHA2OY, MHA2RB), roughDalClusterMap.get(DAL_CLUSTER)));
        Assert.assertTrue(AllTests.weakEquals(Arrays.asList(MHA1OY, MHA1RB1, MHA2OY), roughDalClusterMap.get(DAL_CLUSTER2)));
        Assert.assertTrue(AllTests.weakEquals(Arrays.asList(MHA1OY, MHA1RB1), roughDalClusterMap.get(DAL_CLUSTER3)));
    }

    @Test
    public void testGenerateRealDalClusterMap() throws SQLException {
        setMha2Group(BooleanEnum.FALSE);

        Map<String, List<String>> roughRealDalClusterMap = new HashMap<>() {{
           put(DAL_CLUSTER, Arrays.asList(MHA1OY, MHA1RB1, MHA2OY, MHA2RB));
           put(DAL_CLUSTER2, Arrays.asList(MHA1OY, MHA1RB1, MHA2OY));
           put(DAL_CLUSTER3, Arrays.asList(MHA1OY, MHA1RB1, MHA1RB2));
        }};
        Map<String, List<String>> realDalClusterMap = metaInfoService.generateRealDalClusterMap(roughRealDalClusterMap);
        Assert.assertTrue(AllTests.weakEquals(Sets.newHashSet(Arrays.asList(DAL_CLUSTER, DAL_CLUSTER2, DAL_CLUSTER3)), realDalClusterMap.keySet()));
        Assert.assertTrue(AllTests.weakEquals(Arrays.asList(MHA1OY, MHA1RB1, MHA2OY, MHA2RB), realDalClusterMap.get(DAL_CLUSTER)));
        Assert.assertTrue(AllTests.weakEquals(Arrays.asList(MHA1OY, MHA1RB1), realDalClusterMap.get(DAL_CLUSTER2)));
        Assert.assertTrue(AllTests.weakEquals(Arrays.asList(MHA1OY, MHA1RB1, MHA1RB2), realDalClusterMap.get(DAL_CLUSTER3)));

        setMha2Group(BooleanEnum.TRUE);
    }

    private void setMha2Group(BooleanEnum deteted) throws SQLException {
        MhaTbl mhaTbl1 = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> p.getMhaName().equalsIgnoreCase(MHA2OY)).findFirst().get();
        MhaTbl mhaTbl2 = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> p.getMhaName().equalsIgnoreCase(MHA2RB)).findFirst().get();
        MhaGroupTbl mhaGroupTbl = dalUtils.getMhaGroupTblDao().queryByPk(3L);
        List<GroupMappingTbl> groupMappingTbls = dalUtils.getGroupMappingTblDao().queryAll().stream().filter(p -> p.getMhaGroupId().equals(mhaGroupTbl.getId())).collect(Collectors.toList());
        groupMappingTbls.forEach(g -> g.setDeleted(deteted.getCode()));
        mhaTbl1.setDeleted(deteted.getCode());
        mhaTbl2.setDeleted(deteted.getCode());
        mhaGroupTbl.setDeleted(deteted.getCode());
        dalUtils.getGroupMappingTblDao().batchUpdate(groupMappingTbls);
        dalUtils.getMhaTblDao().batchUpdate(Arrays.asList(mhaTbl1, mhaTbl2));
        dalUtils.getMhaGroupTblDao().update(mhaGroupTbl);
    }

    @Test
    public void testGetUuidMap() throws IOException, SAXException {
        Drc drc = DefaultSaxParser.parse(DRC_XML_one2many);
        Mockito.doReturn(drc).when(dbClusterSourceProvider).getDrc();
        Mockito.doReturn("dc2").when(dbClusterSourceProvider).getLocalDcName();

        Map<String, Set<String>> uuidMap = metaInfoService.getUuidMap();
        Assert.assertEquals(3, uuidMap.size());
        Set<String> mha1dc2 = uuidMap.get("mha1dc2");
        Set<String> mha2dc2 = uuidMap.get("mha2dc2");
        Set<String> mha3dc2 = uuidMap.get("mha3dc2");
        Assert.assertEquals(1, mha1dc2.size());
        Assert.assertEquals(3, mha2dc2.size());
        Assert.assertEquals(3, mha3dc2.size());
        Assert.assertTrue(mha1dc2.contains("92345678-1234-abcd-abcd-123456789abc"));
        Assert.assertTrue(mha2dc2.contains("13345678-1234-abcd-abcd-123456789abc"));

        Assert.assertTrue(mha2dc2.contains("ali_uuid_backup"));
        Assert.assertTrue(mha2dc2.contains("72345678-1234-abcd-abcd-123456789abc"));
        Assert.assertTrue(mha2dc2.contains("13345678-1234-abcd-abcd-123456789abc"));

        Assert.assertTrue(mha3dc2.contains("14345678-1234-abcd-abcd-123456789abc"));
        Assert.assertTrue(mha3dc2.contains("72345678-1234-abcd-abcd-123456789abc"));
        Assert.assertTrue(mha3dc2.contains("ali_uuid_backup"));



    }

    @Test
    public void testGetProxyRoutes() {
        List<RouteDto> proxyRoutes = metaInfoService.getRoutes(null, null, null, null);
        Assert.assertEquals(2, proxyRoutes.size());
        for (RouteDto dto : proxyRoutes) {
            Assert.assertEquals("shaoy", dto.getSrcDcName());
            Assert.assertEquals("sharb", dto.getDstDcName());
            Assert.assertEquals("BBZ", dto.getRouteOrgName());
            Assert.assertTrue("meta".equalsIgnoreCase(dto.getTag()) || "console".equalsIgnoreCase(dto.getTag()));
            List<String> srcProxyIps = dto.getSrcProxyUris();
            List<String> dstProxyIps = dto.getDstProxyUris();
            if ("meta".equalsIgnoreCase(dto.getTag())) {
                Assert.assertEquals(2, srcProxyIps.size());
                Assert.assertEquals(2, dstProxyIps.size());
                Assert.assertTrue(srcProxyIps.contains(PROXY_DC1_1));
                Assert.assertTrue(srcProxyIps.contains(PROXY_DC1_2));
                Assert.assertTrue(dstProxyIps.contains(PROXYTLS_DC2_1));
                Assert.assertTrue(dstProxyIps.contains(PROXYTLS_DC2_2));
            } else {
                Assert.assertEquals(1, srcProxyIps.size());
                Assert.assertEquals(1, dstProxyIps.size());
                Assert.assertTrue(srcProxyIps.contains(PROXY_DC1_1));
                Assert.assertTrue(dstProxyIps.contains(PROXYTLS_DC2_1));
            }
        }

        proxyRoutes = metaInfoService.getRoutes("BBZ", "shaoy", "sharb", "meta");
        Assert.assertEquals(1, proxyRoutes.size());
        for (RouteDto dto : proxyRoutes) {
            Assert.assertEquals("shaoy", dto.getSrcDcName());
            Assert.assertEquals("sharb", dto.getDstDcName());
            Assert.assertEquals("BBZ", dto.getRouteOrgName());
            Assert.assertTrue("meta".equalsIgnoreCase(dto.getTag()) || "console".equalsIgnoreCase(dto.getTag()));
            List<String> srcProxyIps = dto.getSrcProxyUris();
            List<String> dstProxyIps = dto.getDstProxyUris();
            Assert.assertEquals(2, srcProxyIps.size());
            Assert.assertEquals(2, dstProxyIps.size());
            Assert.assertTrue(srcProxyIps.contains(PROXY_DC1_1));
            Assert.assertTrue(srcProxyIps.contains(PROXY_DC1_2));
            Assert.assertTrue(dstProxyIps.contains(PROXYTLS_DC2_1));
            Assert.assertTrue(dstProxyIps.contains(PROXYTLS_DC2_2));
        }

        proxyRoutes = metaInfoService.getRoutes("nosuchorg", "shaoy", "sharb", "meta");
        Assert.assertEquals(0, proxyRoutes.size());

        proxyRoutes = metaInfoService.getRoutes("", "", "", "");
        Assert.assertEquals(0, proxyRoutes.size());
    }


    @Test
    public void testGetAllOrderedDeletedGroupPairs() throws SQLException{
        List<MhaGroupPairVo> allOrderedDeletedGroupPairs = metaInfoService.getAllOrderedDeletedGroupPairs();
        Assert.assertEquals(1,allOrderedDeletedGroupPairs.size());
    }

    @Test
    public void testTestGetXmlConfiguration() throws Exception {
        metaInfoService.getXmlConfiguration(MHA2OY,MHA2RB,BooleanEnum.TRUE);
    }

    @Test
    public void getResources(){
        List<String> resources = metaInfoService.getResources("R");
        Assert.assertTrue(resources.size() > 0);
    }

    @Test
    public void updateMhaDc() {
        int result = metaInfoService.updateMhaDc("fat-fx-drc1", "shaoy");
        Assert.assertEquals(1, result);
    }
}
