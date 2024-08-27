package com.ctrip.framework.drc.console.service.v2;

import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getApplierGroupTblV2s;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getBuTbl;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getColumnsFilterTbl;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getDbReplicationTbls;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getDbReplicationTbls1;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getDbTbls;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getDcTbls;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getFilterMappings;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getMhaDbMappingTbls1;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getMhaReplicationTbl;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getMhaReplicationTbls;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getMhaTblV2;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getMhaTblV2s;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getResourceTbls;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getRowsFilterCreateParam;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getRowsFilterTbl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.BuTblDao;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.MessengerGroupTblDao;
import com.ctrip.framework.drc.console.dao.MessengerTblDao;
import com.ctrip.framework.drc.console.dao.ProxyTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.RouteTblDao;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierGroupTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierGroupTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.MhaDbReplicationTbl;
import com.ctrip.framework.drc.console.dao.v2.ApplierGroupTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.ApplierTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.ColumnsFilterTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.ReplicationTableTblDao;
import com.ctrip.framework.drc.console.dao.v2.RowsFilterTblV2Dao;
import com.ctrip.framework.drc.console.dao.v3.ApplierGroupTblV3Dao;
import com.ctrip.framework.drc.console.dto.v2.MachineDto;
import com.ctrip.framework.drc.console.dto.v3.ReplicatorInfoDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.log.CflBlacklistType;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.param.v2.ColumnsFilterCreateParam;
import com.ctrip.framework.drc.console.param.v2.DbReplicationBuildParam;
import com.ctrip.framework.drc.console.param.v2.DrcBuildBaseParam;
import com.ctrip.framework.drc.console.param.v2.DrcBuildParam;
import com.ctrip.framework.drc.console.param.v2.DrcMhaBuildParam;
import com.ctrip.framework.drc.console.param.v2.GtidCompensateParam;
import com.ctrip.framework.drc.console.param.v2.MessengerMhaBuildParam;
import com.ctrip.framework.drc.console.param.v2.RowsFilterCreateParam;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceSelectParam;
import com.ctrip.framework.drc.console.param.v2.security.Account;
import com.ctrip.framework.drc.console.service.log.ConflictLogService;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.impl.DrcBuildServiceV2Impl;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.service.v2.security.AccountService;
import com.ctrip.framework.drc.console.service.v2.security.KmsService;
import com.ctrip.framework.drc.console.service.v2.security.MetaAccountService;
import com.ctrip.framework.drc.console.vo.v2.ColumnsConfigView;
import com.ctrip.framework.drc.console.vo.v2.DbReplicationView;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.console.vo.v2.RowsFilterConfigView;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.utils.FileUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.util.CollectionUtils;
import org.xml.sax.SAXException;

/**
 * Created by dengquanliang
 * 2023/8/11 21:58
 */
public class DrcBuildServiceV2Test {
    @InjectMocks
    private DrcBuildServiceV2Impl drcBuildServiceV2;
    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;
    @Mock
    private MetaInfoServiceV2 metaInfoService;
    @Mock
    private MhaDbMappingService mhaDbMappingService;
    @Mock
    private MhaTblV2Dao mhaTblDao;
    @Mock
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Mock
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Mock
    private ReplicatorTblDao replicatorTblDao;
    @Mock
    private ApplierGroupTblV2Dao applierGroupTblDao;
    @Mock
    private ApplierTblV2Dao applierTblDao;
    @Mock
    private ApplierGroupTblV3Dao dbApplierGroupTblDao;
    @Mock
    private ResourceTblDao resourceTblDao;
    @Mock
    private DbTblDao dbTblDao;
    @Mock
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Mock
    private DbReplicationTblDao dbReplicationTblDao;
    @Mock
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Mock
    private ColumnsFilterTblV2Dao columnFilterTblV2Dao;
    @Mock
    private RowsFilterTblV2Dao rowsFilterTblV2Dao;
    @Mock
    private BuTblDao buTblDao;
    @Mock
    private DcTblDao dcTblDao;
    @Mock
    private RouteTblDao routeTblDao;
    @Mock
    private ProxyTblDao proxyTblDao;
    @Mock
    private CacheMetaService cacheMetaService;
    @Mock
    private MetaProviderV2 metaProviderV2;

    @Mock
    private MessengerGroupTblDao messengerGroupTblDao;
    @Mock
    private MysqlServiceV2 mysqlServiceV2;
    @Mock
    private ResourceService resourceService;
    @Mock
    private DbaApiService dbaApiService;
    @Mock
    private MachineTblDao machineTblDao;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private MessengerTblDao messengerTblDao;
    @Mock
    private ConflictLogService conflictLogService;

    @Mock
    private MhaDbReplicationService mhaDbReplicationService;

    @Mock
    private ReplicationTableTblDao replicationTableTblDao;
    @Mock
    private MhaServiceV2 mhaServiceV2;
    @Mock
    private AccountService accountService;
    @Mock
    private KmsService kmsService;
    @Mock
    private MetaAccountService metaAccountService;
    @Mock
    private DbDrcBuildService dbDrcBuildService;
    
    
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Mockito.when(accountService.encrypt(Mockito.anyString())).thenReturn("encryptToken");
        Mockito.when(accountService.encrypt(Mockito.anyString())).thenReturn("encryptToken");
    }

    @Test
    public void testBuildMha() throws Exception {
        Mockito.when(buTblDao.queryByBuName(Mockito.anyString())).thenReturn(getBuTbl());
        Mockito.when(dcTblDao.queryByDcName(Mockito.anyString())).thenReturn(getDcTbls().get(0));
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.anyString())).thenReturn(null);
        Mockito.when(mhaTblDao.insertWithReturnId(Mockito.any(MhaTblV2.class))).thenReturn(1L);
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyLong())).thenReturn(null);
        Mockito.when(mhaReplicationTblDao.insert(Mockito.any(MhaReplicationTbl.class))).thenReturn(1);
        Mockito.when(consoleConfig.getAccountKmsTokenSwitch()).thenReturn(true);
        Mockito.when(consoleConfig.getAccountKmsTokenSwitchV2()).thenReturn(true);
        Mockito.when(consoleConfig.getAccountFromMetaSwitch()).thenReturn(true);
        Mockito.when(consoleConfig.getDefaultMonitorAccountKmsToken()).thenReturn("token");
        Mockito.when(consoleConfig.getDefaultReadAccountKmsToken()).thenReturn("token");
        Mockito.when(consoleConfig.getDefaultWriteAccountKmsToken()).thenReturn("token");
        Mockito.when(kmsService.getAccountInfo(Mockito.anyString())).thenReturn(new Account("root","root"));
        MachineDto masterInSrcMha = new MachineDto(3306,"ip1",true);
        MachineDto masterInDstMha = new MachineDto(3306,"ip2",true);
        Mockito.when(accountService.mhaAccountV2ChangeAndRecord(Mockito.any(MhaTblV2.class),Mockito.anyString(),Mockito.anyInt())).thenReturn(true);
        


        drcBuildServiceV2.buildMhaAndReplication(new DrcMhaBuildParam(
                "srcMha", "dstMha", 
                "srcDc", "dstDc", 
                "BBZ", "srcTag", "dstTag",
                Lists.newArrayList(masterInSrcMha),
                Lists.newArrayList(masterInDstMha)
        ));
        Mockito.verify(mhaTblDao, Mockito.never()).update(Mockito.any(MhaTblV2.class));
        Mockito.verify(mhaTblDao, Mockito.times(2)).insertWithReturnId(Mockito.any(MhaTblV2.class));

        drcBuildServiceV2.buildMhaAndReplication(new DrcMhaBuildParam(
                "srcMha", "dstMha",
                "srcDc", "dstDc",
                "BBZ", "srcTag", "dstTag",
                null,
                null
        ));
        Mockito.verify(mhaTblDao, Mockito.times(4)).insertWithReturnId(Mockito.any(MhaTblV2.class));
    }


    @Test
    public void testBuildAmbiguousMha() throws Exception {
        Mockito.when(buTblDao.queryByBuName(Mockito.anyString())).thenReturn(getBuTbl());
        Mockito.when(dcTblDao.queryByDcName(Mockito.anyString())).thenReturn(getDcTbls().get(0));
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.anyString())).thenReturn(null);
        Mockito.when(mhaTblDao.insertWithReturnId(Mockito.any(MhaTblV2.class))).thenReturn(1L);
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyLong())).thenReturn(null);
        Mockito.when(mhaReplicationTblDao.insert(Mockito.any(MhaReplicationTbl.class))).thenReturn(1);
        Mockito.when(consoleConfig.getAccountKmsTokenSwitch()).thenReturn(true);
        Mockito.when(consoleConfig.getAccountKmsTokenSwitchV2()).thenReturn(true);
        Mockito.when(consoleConfig.getAccountFromMetaSwitch()).thenReturn(true);
        Mockito.when(consoleConfig.getDefaultMonitorAccountKmsToken()).thenReturn("token");
        Mockito.when(consoleConfig.getDefaultReadAccountKmsToken()).thenReturn("token");
        Mockito.when(consoleConfig.getDefaultWriteAccountKmsToken()).thenReturn("token");
        // for ambiguous mha
        Mockito.when(consoleConfig.getAllowAmbiguousMhaSwitch()).thenReturn(true);
        MachineTbl machineTbl = new MachineTbl();
        machineTbl.setMhaId(111L);
        Mockito.when(machineTblDao.queryByIpPort(Mockito.anyString(), Mockito.eq(13306))).thenReturn(machineTbl);
        Mockito.when(mhaTblDao.queryById(Mockito.eq(111L))).thenReturn(new MhaTblV2());
        
        Mockito.when(kmsService.getAccountInfo(Mockito.anyString())).thenReturn(new Account("root","root"));
        MachineDto masterInSrcMha = new MachineDto(13306,"ip1",true);
        MachineDto masterInDstMha = new MachineDto(13307,"ip2",true);
        MachineDto slaveInDstMha = new MachineDto(13306,"ip3",false);

        Mockito.when(accountService.mhaAccountV2ChangeAndRecord(Mockito.any(MhaTblV2.class),Mockito.anyString(),Mockito.anyInt())).thenReturn(true);

        drcBuildServiceV2.buildMhaAndReplication(new DrcMhaBuildParam(
                "srcMha", "dstMha",
                "srcDc", "dstDc",
                "BBZ", "srcTag", "dstTag",
                Lists.newArrayList(masterInSrcMha),
                Lists.newArrayList(masterInDstMha,slaveInDstMha)
        ));
        Mockito.verify(mhaTblDao, Mockito.never()).update(Mockito.any(MhaTblV2.class));
        Mockito.verify(mhaTblDao, Mockito.times(2)).insertWithReturnId(Mockito.any(MhaTblV2.class));

        drcBuildServiceV2.buildMhaAndReplication(new DrcMhaBuildParam(
                "srcMha", "dstMha",
                "srcDc", "dstDc",
                "BBZ", "srcTag", "dstTag",
                null,
                null
        ));
        Mockito.verify(mhaTblDao, Mockito.times(4)).insertWithReturnId(Mockito.any(MhaTblV2.class));
        Mockito.verify(machineTblDao, Mockito.times(3)).queryByIpPort(Mockito.anyString(), Mockito.anyInt());


        Mockito.when(consoleConfig.getAllowAmbiguousMhaSwitch()).thenReturn(false);
        try {
            drcBuildServiceV2.buildMhaAndReplication(new DrcMhaBuildParam(
                    "srcMha", "dstMha",
                    "srcDc", "dstDc",
                    "BBZ", "srcTag", "dstTag",
                    Lists.newArrayList(masterInSrcMha),
                    Lists.newArrayList(masterInDstMha,slaveInDstMha)
            ));
        }catch (Exception e){
            Assert.assertTrue(e instanceof ConsoleException);
            Assert.assertTrue(e.getMessage().contains("ambiguousMha"));
        }
        
    }
    

    @Test
    public void testBuildDrc() throws Exception {
        DrcBuildBaseParam srcParam = new DrcBuildBaseParam("srcMha", Lists.newArrayList("127.0.0.1"),
                Lists.newArrayList("127.0.0.1"), "rGtid", "aGtid");
        DrcBuildBaseParam dstParam = new DrcBuildBaseParam("dstMha", Lists.newArrayList("127.0.0.1"),
                Lists.newArrayList("127.0.0.1"), "rGtid", "aGtid");
        DrcBuildParam param = new DrcBuildParam();
        param.setSrcBuildParam(srcParam);
        param.setDstBuildParam(dstParam);
        List<MhaTblV2> mhaTblV2s = getMhaTblV2s();

        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("srcMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("dstMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        Mockito.when(mhaTblDao.update(Mockito.any(MhaTblV2.class))).thenReturn(1);

        Mockito.when(replicatorGroupTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(null);
        Mockito.when(replicatorGroupTblDao.insertWithReturnId(Mockito.any())).thenReturn(200L);

        Mockito.when(applierGroupTblDao.queryByMhaReplicationId(Mockito.anyLong())).thenReturn(null);
        Mockito.when(applierGroupTblDao.insertWithReturnId(Mockito.any())).thenReturn(200L);

        Mockito.when(replicatorTblDao.queryByRGroupIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(new ArrayList<>());
        Mockito.when(replicatorTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[1]);

        Mockito.when(applierTblDao.queryByApplierGroupId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(new ArrayList<>());
        Mockito.when(applierTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[1]);

        Mockito.when(resourceTblDao.queryByIps(Mockito.anyList())).thenReturn(getResourceTbls());
        Mockito.when(mhaReplicationTblDao.insertWithReturnId(Mockito.any())).thenReturn(200L);
        Mockito.when(mhaReplicationTblDao.insert(Mockito.any(MhaReplicationTbl.class))).thenReturn(1);
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyInt())).thenReturn(getMhaReplicationTbls().get(0));
        Mockito.when(mhaReplicationTblDao.update(Mockito.any(MhaReplicationTbl.class))).thenReturn(1);
        Mockito.doNothing().when(metaProviderV2).scheduledTask();
        Mockito.when(metaInfoService.getDrcReplicationConfig(Mockito.anyString(), Mockito.anyString())).thenReturn(new Drc());

        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(getMhaDbMappingTbls1());
        Mockito.when(dbReplicationTblDao.queryByMappingIds(Mockito.anyList(), Mockito.anyList(), Mockito.anyInt())).thenReturn(getDbReplicationTbls());

        drcBuildServiceV2.buildDrc(param);

        Mockito.verify(replicatorTblDao, Mockito.never()).batchUpdate(Mockito.anyList());
        Mockito.verify(replicatorTblDao, Mockito.times(2)).batchInsert(Mockito.anyList());
        Mockito.verify(applierTblDao, Mockito.never()).batchUpdate(Mockito.anyList());
        Mockito.verify(applierTblDao, Mockito.times(2)).batchInsert(Mockito.anyList());

        Mockito.verify(replicatorGroupTblDao, Mockito.times(2)).insertWithReturnId(Mockito.any());
        Mockito.verify(replicatorGroupTblDao, Mockito.never()).update(Mockito.anyList());
        Mockito.verify(applierGroupTblDao, Mockito.times(2)).insertWithReturnId(Mockito.any());
        Mockito.verify(applierGroupTblDao, Mockito.never()).update(Mockito.anyList());

        Mockito.verify(mhaReplicationTblDao, Mockito.times(2)).update(Mockito.any(MhaReplicationTbl.class));
        Mockito.verify(mhaTblDao, Mockito.never()).update(Mockito.any(MhaTblV2.class));
    }

    @Test
    public void testConfigureDbReplications() throws Exception {
        DbReplicationBuildParam param = new DbReplicationBuildParam("srcMha", "dstMha", "db200", "table");

        List<MhaTblV2> mhaTblV2s = getMhaTblV2s();
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("srcMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("dstMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));

        List<String> dbList = Lists.newArrayList("db200");
        List<String> tableList = Lists.newArrayList("db200.table");
        Mockito.when(mhaDbMappingService.initMhaDbMappings(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(Pair.of(dbList, tableList));

        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(getMhaDbMappingTbls1());
        Mockito.when(dbTblDao.queryByDbNames(Mockito.anyList())).thenReturn(getDbTbls());
        Mockito.when(dbTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbTbls());
        Mockito.when(dbReplicationTblDao.queryByMappingIds(Mockito.anyList(), Mockito.anyList(), Mockito.anyInt())).thenReturn(getDbReplicationTbls1());
        Mockito.when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.anyString(), Mockito.anyString())).thenReturn(Lists.newArrayList("db200.table"));
        Mockito.when(dbReplicationTblDao.batchInsertWithReturnId(Mockito.anyList())).thenReturn(getDbReplicationTbls1());
        Mockito.when(consoleConfig.getCflBlackListAutoAddSwitch()).thenReturn(true);
        try {
            Mockito.doNothing().when(conflictLogService).addDbBlacklist(Mockito.anyString(), Mockito.eq(
                    CflBlacklistType.NEW_CONFIG),Mockito.any());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Mockito.when(mhaServiceV2.getRegion(Mockito.anyString())).thenReturn("region");
        Mockito.when(replicationTableTblDao.queryByDbReplicationIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(new ArrayList<>());
        Mockito.when(replicationTableTblDao.insert(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(replicationTableTblDao.update(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.doNothing().when(mhaDbReplicationService).maintainMhaDbReplication(Mockito.anyList());
        List<Long> results = drcBuildServiceV2.configureDbReplications(param);
        Mockito.verify(dbReplicationTblDao, Mockito.times(1)).batchInsertWithReturnId(Mockito.any());

    }

    @Test
    public void testBuildDbReplicationConfig() throws Exception {
        DbReplicationBuildParam param = new DbReplicationBuildParam("srcMha", "dstMha", "db", "table");
        ColumnsFilterCreateParam columnsFilterCreateParam = new ColumnsFilterCreateParam(Lists.newArrayList(200L, 201L), 0, Lists.newArrayList("column"));
        RowsFilterCreateParam rowsFilterCreateParam = getRowsFilterCreateParam();

        List<DbReplicationTbl> dbReplicationTbls = getDbReplicationTbls();
        List<DbReplicationTbl> dbReplicationTbls0 = dbReplicationTbls.stream().filter(e -> e.getId() == 200L).collect(Collectors.toList());

        List<Long> dbReplicationIds = dbReplicationTbls0.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
        param.setDbReplicationIds(dbReplicationIds);

        Mockito.when(dbReplicationTblDao.queryByIds(dbReplicationIds)).thenReturn(dbReplicationTbls0);
        List<MhaTblV2> mhaTblV2s = getMhaTblV2s();
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("srcMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("dstMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(getMhaDbMappingTbls1());
        Mockito.when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.anyString(), Mockito.anyString())).thenReturn(Lists.newArrayList("db200.table"));
        Mockito.when(dbTblDao.queryByDbNames(Mockito.anyList())).thenReturn(getDbTbls());
        Mockito.when(dbTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbTbls());
        Mockito.when(dbReplicationTblDao.update(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());
        Mockito.when(rowsFilterTblV2Dao.queryById(Mockito.anyLong())).thenReturn(getRowsFilterTbl());
        Mockito.when(columnFilterTblV2Dao.queryById(Mockito.anyLong())).thenReturn(getColumnsFilterTbl());
        Mockito.when(mysqlServiceV2.getCommonColumnIn(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(Sets.newHashSet("udl", "uid", "column"));
        Mockito.when(consoleConfig.getCflBlackListAutoAddSwitch()).thenReturn(false);

        Mockito.when(mhaServiceV2.getRegion(Mockito.anyString())).thenReturn("region");
        Mockito.when(replicationTableTblDao.queryByDbReplicationIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(new ArrayList<>());
        Mockito.when(replicationTableTblDao.insert(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(replicationTableTblDao.update(Mockito.anyList())).thenReturn(new int[1]);


        drcBuildServiceV2.buildDbReplicationConfig(param);
        param.setColumnsFilterCreateParam(columnsFilterCreateParam);
        param.setRowsFilterCreateParam(rowsFilterCreateParam);
        drcBuildServiceV2.buildDbReplicationConfig(param);
    }


//    @Test
//    public void testUpdateDbReplications() throws Exception {
//        DbReplicationBuildParam param = new DbReplicationBuildParam("srcMha", "dstMha", "db", "table");
//        List<DbReplicationTbl> dbReplicationTbls = getDbReplicationTbls();
//        List<DbReplicationTbl> dbReplicationTbls0 = dbReplicationTbls.stream().filter(e -> e.getId() == 200L).collect(Collectors.toList());
//
//        List<Long> dbReplicationIds = dbReplicationTbls0.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
//        param.setDbReplicationIds(dbReplicationIds);
//
//        Mockito.when(dbReplicationTblDao.queryByIds(dbReplicationIds)).thenReturn(dbReplicationTbls0);
//        List<MhaTblV2> mhaTblV2s = getMhaTblV2s();
//        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("srcMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
//        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("dstMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
//        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(getMhaDbMappingTbls1());
//        Mockito.when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.anyString(), Mockito.anyString())).thenReturn(Lists.newArrayList("test.table"));
//        Mockito.when(dbTblDao.queryByDbNames(Mockito.anyList())).thenReturn(getDbTbls());
//        Mockito.when( dbReplicationTblDao.update(Mockito.anyList())).thenReturn(new int[1]);
//        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());
//        Mockito.when(rowsFilterTblV2Dao.queryById(Mockito.anyLong())).thenReturn(getRowsFilterTbl());
//        Mockito.when(columnFilterTblV2Dao.queryById(Mockito.anyLong())).thenReturn(getColumnsFilterTbl());
//        Mockito.when(mysqlServiceV2.getCommonColumnIn(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(Sets.newHashSet("udl", "uid", "column"));
//
//        List<Long> results = drcBuildServiceV2.configureDbReplications(param);
//        Assert.assertEquals(results.size(), dbReplicationIds.size());
//    }

    @Test
    public void testGetDbReplicationView() throws Exception {
        List<MhaTblV2> mhaTblV2s = getMhaTblV2s();
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("srcMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("dstMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));

        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(getMhaDbMappingTbls1());
        Mockito.when(dbReplicationTblDao.queryByMappingIds(Mockito.anyList(), Mockito.anyList(), Mockito.anyInt())).thenReturn(getDbReplicationTbls());
        Mockito.when(dbTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbTbls());
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());

        List<DbReplicationView> result = drcBuildServiceV2.getDbReplicationView("srcMha", "dstMha");
        Assert.assertEquals(result.size(), 2);
    }

    @Test
    public void testDeleteDbReplications() throws Exception {
        Mockito.when(dbReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbReplicationTbls());
        Mockito.when(dbReplicationTblDao.batchUpdate(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(dbReplicationFilterMappingTblDao.batchUpdate(Mockito.anyList())).thenReturn(new int[1]);

        drcBuildServiceV2.deleteDbReplications(Lists.newArrayList(200L, 201L));
        Mockito.verify(dbReplicationTblDao, Mockito.times(1)).batchUpdate(Mockito.any());
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.never()).batchUpdate(Mockito.any());
        Mockito.verify(mhaDbReplicationService, Mockito.times(1)).offlineMhaDbReplication(Mockito.anyList());
    }

    @Test
    public void testBuildColumnsFilter() throws Exception {
        ColumnsFilterCreateParam param = new ColumnsFilterCreateParam(Lists.newArrayList(200L, 201L), 0, Lists.newArrayList("column"));
        Mockito.when(dbReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbReplicationTbls());
        Mockito.when(columnFilterTblV2Dao.queryByMode(Mockito.anyInt())).thenReturn(new ArrayList<>());
        Mockito.when(columnFilterTblV2Dao.insertReturnId(Mockito.any())).thenReturn(200L);
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationId(Mockito.anyLong())).thenReturn(new ArrayList<>());

        drcBuildServiceV2.buildColumnsFilter(param);
        Mockito.verify(columnFilterTblV2Dao, Mockito.times(1)).insertReturnId(Mockito.any());
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.times(1)).batchInsert(Mockito.anyList());
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.never()).batchUpdate(Mockito.anyList());

    }

    @Test
    public void testGetColumnsConfigView() throws Exception {
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationId(Mockito.anyLong())).thenReturn(getFilterMappings());
        Mockito.when(columnFilterTblV2Dao.queryById(Mockito.anyLong())).thenReturn(getColumnsFilterTbl());

        ColumnsConfigView result = drcBuildServiceV2.getColumnsConfigView(200L);
        Assert.assertEquals(result.getColumns().size(), 1);
    }

    @Test
    public void testDeleteColumnsFilter() throws Exception {
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());
        Mockito.when(dbReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbReplicationTbls());
        Mockito.when(dbReplicationFilterMappingTblDao.batchUpdate(Mockito.anyList())).thenReturn(new int[1]);

        drcBuildServiceV2.deleteColumnsFilter(Lists.newArrayList(200L, 201L));
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.times(1)).batchUpdate(Mockito.anyList());
    }

    @Test
    public void testGetRowsConfigView() throws Exception {
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationId(Mockito.anyLong())).thenReturn(getFilterMappings());
        Mockito.when(rowsFilterTblV2Dao.queryById(Mockito.anyLong())).thenReturn(getRowsFilterTbl());

        RowsFilterConfigView result = drcBuildServiceV2.getRowsConfigView(200L);
        Assert.assertTrue(CollectionUtils.isEmpty(result.getColumns()));
        Assert.assertEquals(result.getUdlColumns().size(), 1);
        Assert.assertEquals(result.getMode(), 1);
    }

    @Test
    public void testDeleteRowsFilter() throws Exception {
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());
        Mockito.when(dbReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbReplicationTbls());
        Mockito.when(dbReplicationFilterMappingTblDao.batchUpdate(Mockito.anyList())).thenReturn(new int[1]);

        drcBuildServiceV2.deleteRowsFilter(Lists.newArrayList(200L, 201L));
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.times(1)).batchUpdate(Mockito.anyList());
    }

    @Test
    public void testBuildRowsFilter() throws Exception {
        RowsFilterCreateParam param = getRowsFilterCreateParam();
        Mockito.when(dbReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbReplicationTbls());
        Mockito.when(rowsFilterTblV2Dao.queryByMode(Mockito.anyInt())).thenReturn(new ArrayList<>());
        Mockito.when(rowsFilterTblV2Dao.insertReturnId(Mockito.any())).thenReturn(200L);
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationId(Mockito.anyLong())).thenReturn(new ArrayList<>());

        drcBuildServiceV2.buildRowsFilter(param);
        Mockito.verify(rowsFilterTblV2Dao, Mockito.times(1)).insertReturnId(Mockito.any());
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.times(1)).batchInsert(Mockito.anyList());
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.never()).batchUpdate(Mockito.anyList());
    }

    @Test
    public void testGetApplierGtid() throws Exception {
        List<MhaTblV2> mhaTblV2s = getMhaTblV2s();
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("srcMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("dstMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyLong())).thenReturn(getMhaReplicationTbl());
        Mockito.when(applierGroupTblDao.queryByMhaReplicationId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(getApplierGroupTblV2s().get(0));

        String result = drcBuildServiceV2.getApplierGtid("srcMha", "dstMha");
        Assert.assertEquals(result, getApplierGroupTblV2s().get(0).getGtidInit());
    }

    @Test
    public void testBuildMessengerMha() throws Exception {
        List<DcTbl> dcTbls = getDcTbls();
        Mockito.when(dcTblDao.queryByDcName(Mockito.anyString())).thenReturn(dcTbls.get(0));
        Mockito.when(buTblDao.queryByBuName(Mockito.anyString())).thenReturn(getBuTbl());
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.anyString())).thenReturn(getMhaTblV2());
        Mockito.when(messengerGroupTblDao.upsertIfNotExist(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString())).thenReturn(1L);

        // messengerGroup
//        Long srcReplicatorGroupId = replicatorGroupTblDao.upsertIfNotExist(1L);
        MessengerMhaBuildParam param = new MessengerMhaBuildParam();
        param.setBuName("bu");
        param.setMhaName("mha");
        param.setDc("dc");
        drcBuildServiceV2.buildMessengerMha(param);
    }


    @Test
    public void testAutoConfigReplicatorsWithRealTimeGtid() throws Exception {
        Mockito.when(replicatorGroupTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(MockEntityBuilder.buildReplicatorGroupTbl(1L, 1L));
        Mockito.when(replicatorTblDao.queryByRGroupIds(Mockito.anyList(), Mockito.eq(0))).thenReturn(Lists.newArrayList());
        List<ResourceView> resourceViews = MockEntityBuilder.buildResourceViews(2, ModuleEnum.REPLICATOR.getCode());
        Mockito.when(resourceService.autoConfigureResource(Mockito.any(ResourceSelectParam.class))).thenReturn(resourceViews);
        Mockito.when(mysqlServiceV2.getMhaExecutedGtid(Mockito.anyString())).thenReturn("gtid");
        Mockito.when(metaInfoService.findAvailableApplierPort(Mockito.anyString())).thenReturn(8383);
        Mockito.when(replicatorTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[]{1, 1});
        drcBuildServiceV2.autoConfigReplicatorsWithRealTimeGtid(MockEntityBuilder.buildMhaTblV2());
    }

    @Test
    public void testAutoConfigAppliersWithRealTimeGtid() throws Exception {
        Mockito.when(mysqlServiceV2.getMhaExecutedGtid(Mockito.anyString())).thenReturn("gtid");
        Mockito.when(applierGroupTblDao.update(Mockito.any(ApplierGroupTblV2.class))).thenReturn(1);
        Mockito.when(mhaReplicationTblDao.update(Mockito.any(MhaReplicationTbl.class))).thenReturn(1);
        Mockito.when(resourceService.handOffResource(Mockito.any(ResourceSelectParam.class))).thenReturn(MockEntityBuilder.buildResourceViews(2,
                ModuleEnum.APPLIER.getCode()));
        Mockito.when(applierTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[]{1, 1});
        MhaTblV2 mha1 = MockEntityBuilder.buildMhaTblV2(1L, "mha1", 1L);
        MhaTblV2 mha2 = MockEntityBuilder.buildMhaTblV2(2L, "mha2", 2L);
        MhaReplicationTbl mhaReplicationTbl = MockEntityBuilder.buildMhaReplicationTbl(1L, mha1, mha2);
        ApplierGroupTblV2 applierGroupTblV2 = MockEntityBuilder.buildApplierGroupTbl(1L, mhaReplicationTbl);
        drcBuildServiceV2.autoConfigAppliersWithRealTimeGtid(mhaReplicationTbl, applierGroupTblV2, mha1, mha2);
    }

    @Test
    public void testAutoConfigMessengersWithRealTimeGtid() throws Exception {
        String gtid = "abc-zyn-test:1235";
        Mockito.when(mysqlServiceV2.getMhaExecutedGtid(Mockito.anyString())).thenReturn(gtid);
        Mockito.when(messengerGroupTblDao.queryByMhaId(Mockito.anyLong(), Mockito.eq(0))).thenReturn(MockEntityBuilder.buildMessengerGroupTbl(1L, 1L));
        Mockito.when(messengerGroupTblDao.update(Mockito.any(MessengerGroupTbl.class))).thenReturn(1);
        Mockito.when(resourceService.handOffResource(Mockito.any(ResourceSelectParam.class))).thenReturn(MockEntityBuilder.buildResourceViews(2,
                ModuleEnum.APPLIER.getCode()));
        Mockito.when(messengerTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[]{1, 1});
        drcBuildServiceV2.autoConfigMessengersWithRealTimeGtid(MockEntityBuilder.buildMhaTblV2(),Mockito.anyBoolean());
        Mockito.verify(messengerTblDao,Mockito.times(1)).batchInsert(Mockito.anyList());
        Mockito.verify(messengerGroupTblDao,Mockito.times(1)).update((MessengerGroupTbl) Mockito.argThat(e->{
            MessengerGroupTbl group = (MessengerGroupTbl) e;
            return group.getGtidExecuted().equals(gtid);
        }));
    }


    @Test
    public void testAutoConfigMessengers() throws Exception {
        String gtid = "abc-zyn-test:1235";
        MessengerGroupTbl value = MockEntityBuilder.buildMessengerGroupTbl(1L, 1L);
        value.setGtidExecuted(gtid);
        Mockito.when(messengerGroupTblDao.queryByMhaId(Mockito.anyLong(), Mockito.eq(0))).thenReturn(value);
        Mockito.when(messengerGroupTblDao.update(Mockito.any(MessengerGroupTbl.class))).thenReturn(1);
        MessengerTbl messengerTbl = new MessengerTbl();
        messengerTbl.setResourceId(1001L);
        messengerTbl.setId(101L);
        Mockito.when(messengerTblDao.queryByGroupId(Mockito.eq(1L))).thenReturn(Lists.newArrayList(messengerTbl));
        ResourceTbl resourceTbl = new ResourceTbl();
        resourceTbl.setId(1001L);
        resourceTbl.setIp("10.12.13.14");
        Mockito.when(resourceTblDao.queryByIds(Mockito.anyList())).thenReturn(Lists.newArrayList(resourceTbl));
        Mockito.when(resourceService.handOffResource(Mockito.any(ResourceSelectParam.class))).thenReturn(MockEntityBuilder.buildResourceViews(2,
                ModuleEnum.APPLIER.getCode()));
        Mockito.when(messengerTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[]{1, 1});
        drcBuildServiceV2.autoConfigMessenger(MockEntityBuilder.buildMhaTblV2(),null,false);
        Mockito.verify(messengerTblDao,Mockito.times(1)).batchInsert(Mockito.anyList());
        Mockito.verify(messengerTblDao,Mockito.times(1)).batchUpdate(Mockito.argThat(e->e.stream().allMatch(tbl-> Objects.equals(tbl.getDeleted(), BooleanEnum.TRUE.getCode()))));
        Mockito.verify(messengerGroupTblDao,Mockito.never()).update((MessengerGroupTbl) Mockito.anyObject());
    }

    @Test
    public void testInitReplicationTables() throws Exception {
        Mockito.when(mhaReplicationTblDao.queryAllExist()).thenReturn(PojoBuilder.getMhaReplicationTbls1());
        Mockito.when(mhaTblDao.queryAllExist()).thenReturn(PojoBuilder.getMhaTblV2s());
        Mockito.when(dcTblDao.queryAllExist()).thenReturn(PojoBuilder.getDcTbls());
        Mockito.when(mhaDbMappingTblDao.queryAllExist()).thenReturn(PojoBuilder.getMhaDbMappingTbls2());
        Mockito.when(dbTblDao.queryAllExist()).thenReturn(PojoBuilder.getDbTbls());
        Mockito.when(dbReplicationTblDao.queryByMappingIds(Mockito.anyList(), Mockito.anyList(), Mockito.anyInt())).thenReturn(PojoBuilder.getDbReplicationTbls());
        Mockito.when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.anyString(), Mockito.anyString())).thenReturn(Lists.newArrayList("db200.table1"));

        Mockito.when(replicationTableTblDao.insert(Mockito.anyList())).thenReturn(new int[0]);
        drcBuildServiceV2.initReplicationTables();
        Mockito.verify(replicationTableTblDao, Mockito.times(2)).insert(Mockito.anyList());
    }

    @Test
    public void testCompensateGtidGap() throws Exception {
        DcTbl dc1 = new DcTbl();
        dc1.setId(1L);
        dc1.setRegionName("region1");
        
        MhaTblV2 mha1 = MockEntityBuilder.buildMhaTblV2(1L, "mha1", 1L);
        MhaTblV2 mha2 = MockEntityBuilder.buildMhaTblV2(2L, "mha2", 2L);
        MhaReplicationTbl mha1_mha2 = MockEntityBuilder.buildMhaReplicationTbl(1L, mha1, mha2);
        ApplierGroupTblV2 applierGroupTblV2 = MockEntityBuilder.buildApplierGroupTbl(1L, mha1_mha2);
        MessengerGroupTbl messengerGroupTbl = MockEntityBuilder.buildMessengerGroupTbl(1L, mha1.getId());
        MhaDbReplicationTbl mhaDbReplicationTbl = MockEntityBuilder.buildMhaDbReplicationTbl(1L);
        ApplierGroupTblV3 applierGroupTblV3 = MockEntityBuilder.buildDbApplierGroup(1L,mhaDbReplicationTbl.getId());
        
        
        Mockito.when(mhaTblDao.queryByPk(Mockito.anyList())).thenReturn(Lists.newArrayList(mha1));
        Mockito.when(dcTblDao.queryByPk(Mockito.anyList())).thenReturn(Lists.newArrayList(dc1));
        
        Mockito.when(mysqlServiceV2.getMhaExecutedGtid(Mockito.anyString())).thenReturn("u1:1-22,u2:1-22");
        Mockito.when(mhaReplicationTblDao.queryBySrcMhaId(Mockito.anyLong())).thenReturn(Lists.newArrayList());
        Mockito.when(applierGroupTblDao.queryByMhaReplicationIds(Mockito.anyList())).thenReturn(Lists.newArrayList(applierGroupTblV2));
        Mockito.when(mhaDbReplicationService.queryBySrcMha(Mockito.anyString())).thenReturn(Lists.newArrayList(mhaDbReplicationTbl));
        Mockito.when(dbApplierGroupTblDao.queryByMhaDbReplicationIds(Mockito.anyList())).thenReturn(Lists.newArrayList(applierGroupTblV3));
        Mockito.when(messengerGroupTblDao.queryByMhaId(Mockito.anyLong(),Mockito.anyInt())).thenReturn(messengerGroupTbl);
        
        
        Mockito.when(applierGroupTblDao.update(Mockito.anyList())).thenReturn(new int[]{1});
        Mockito.when(dbApplierGroupTblDao.update(Mockito.anyList())).thenReturn(new int[]{1});
        Mockito.when(messengerGroupTblDao.update(Mockito.anyList())).thenReturn(new int[]{1});

        GtidCompensateParam gtidCompensateParam = new GtidCompensateParam();
        gtidCompensateParam.setSrcMhaIds(Lists.newArrayList(mha1.getId()));
        gtidCompensateParam.setSrcRegion("region1");
        gtidCompensateParam.setExecute(false);
        int affectReplication = drcBuildServiceV2.compensateGtidGap(gtidCompensateParam);
        Assert.assertEquals(3, affectReplication);
        
        
        gtidCompensateParam.setExecute(true);
        affectReplication = drcBuildServiceV2.compensateGtidGap(gtidCompensateParam);
        Assert.assertEquals(3, affectReplication);
        Mockito.verify(applierGroupTblDao,Mockito.times(1)).update(Mockito.anyList());
        Mockito.verify(dbApplierGroupTblDao,Mockito.times(1)).update(Mockito.anyList());
        Mockito.verify(messengerGroupTblDao,Mockito.times(1)).update(Mockito.any(MessengerGroupTbl.class));
        
        
    }


    @Test
    public void testIsolationMigrateReplicator() throws SQLException {
        MhaTblV2 mha1 = MockEntityBuilder.buildMhaTblV2(1L, "mha1", 1L);
        mha1.setTag("tag3");
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.anyString())).thenReturn(mha1);
        Mockito.when(mhaServiceV2.getMhaReplicatorsV2(Mockito.anyString())).thenReturn(
                Lists.newArrayList(
                        new ReplicatorInfoDto(1L,"gtid",true,"ip1","tag1","az1"),
                        new ReplicatorInfoDto(2L,"gtid",false,"ip2","tag1","az2")
                )
        );
        ResourceView resourceView1 = new ResourceView();
        resourceView1.setResourceId(1L);
        resourceView1.setType(ModuleEnum.REPLICATOR.getCode());
        resourceView1.setTag("tag3");
        resourceView1.setAz("az1");
        resourceView1.setIp("ip3");

        ResourceView resourceView2 = new ResourceView();
        resourceView2.setResourceId(2L);
        resourceView2.setType(ModuleEnum.REPLICATOR.getCode());
        resourceView2.setTag("tag3");
        resourceView2.setAz("az2");
        resourceView2.setIp("ip4");
        
        Mockito.when(resourceService.getMhaAvailableResource(Mockito.anyString(),Mockito.anyInt())).thenReturn(
                Lists.newArrayList(
                        resourceView1,
                        resourceView2
                )
        );
        Mockito.when(mysqlServiceV2.getMhaExecutedGtid(Mockito.anyString())).thenReturn("gtidInit");
        Mockito.when(replicatorTblDao.update(Mockito.any(ReplicatorTbl.class))).thenReturn(1);

        int i = drcBuildServiceV2.isolationMigrateReplicator(Lists.newArrayList("mha1"), true, "tag3", null);
        Assert.assertEquals(1,i);


        Mockito.when(mhaServiceV2.getMhaReplicatorsV2(Mockito.anyString())).thenReturn(
                Lists.newArrayList(
                        new ReplicatorInfoDto(1L,"gtid",true,"ip1","tag1","az1"),
                        new ReplicatorInfoDto(2L,"gtid",false,"ip4","tag3","az2")
                )
        );
        
        i = drcBuildServiceV2.isolationMigrateReplicator(Lists.newArrayList("mha1"), false, "tag3", null);
        Assert.assertEquals(1,i);
        

    }

    @Test
    public void testIsolationMigrateApplier() throws Exception {
        MhaTblV2 mha1 = MockEntityBuilder.buildMhaTblV2(1L, "mha1", 1L);
        MhaTblV2 mha2 = MockEntityBuilder.buildMhaTblV2(2L, "mha2", 2L);
        mha2.setTag("tag");
        Mockito.when(mhaReplicationTblDao.queryByDstMhaId(Mockito.anyLong())).thenReturn(
                Lists.newArrayList(
                        MockEntityBuilder.buildMhaReplicationTbl(
                            1L,
                                mha1,
                                mha2
                        )
                )
        );
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.anyString())).thenReturn(mha2);
        Mockito.when(mhaTblDao.queryByPk(Mockito.anyLong())).thenReturn(mha1);
        Mockito.doNothing().when(dbDrcBuildService).switchAppliers(Mockito.anyList());
        Mockito.doNothing().when(dbDrcBuildService).switchMessengers(Mockito.anyList());

        int i = drcBuildServiceV2.isolationMigrateApplier(Lists.newArrayList("mha2"), "tag");
        Assert.assertEquals(1,i);
    }

    @Test
    public void testCheckIsoMigrateStatus() throws SQLException, IOException, SAXException {
        ResourceTbl resource1 = new ResourceTbl();
        resource1.setId(1L);
        resource1.setIp("ip1");

        ResourceTbl resource2 = new ResourceTbl();
        resource2.setId(2L);
        resource2.setIp("ip2");


        InputStream ins = FileUtils.getFileInputStream("newMeta.xml");
        Drc newDrc = DefaultSaxParser.parse(ins);
        
        Mockito.when(resourceTblDao.queryBy(Mockito.any())).thenReturn(Lists.newArrayList(resource1,resource2));
        Mockito.when(metaProviderV2.getDrc()).thenReturn(newDrc);

        Pair<Boolean, String> checkRes = drcBuildServiceV2.checkIsoMigrateStatus(Lists.newArrayList("mha1", "mha2"), "tag");
        Assert.assertFalse(checkRes.getKey());

        resource2.setIp("testip");
        checkRes = drcBuildServiceV2.checkIsoMigrateStatus(Lists.newArrayList("mha1", "mha2"), "tag");
        Assert.assertTrue(checkRes.getKey());
    }
    
    

}
