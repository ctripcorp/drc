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
import static org.mockito.Mockito.*;

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
import com.ctrip.framework.drc.console.service.v2.external.dba.response.Data;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbaClusterInfoResponse;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.MemberInfo;
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
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        when(accountService.encrypt(Mockito.anyString())).thenReturn("encryptToken");
        when(accountService.encrypt(Mockito.anyString())).thenReturn("encryptToken");
    }

    @Test
    public void testBuildMha() throws Exception {
        when(buTblDao.queryByBuName(Mockito.anyString())).thenReturn(getBuTbl());
        when(dcTblDao.queryByDcName(Mockito.anyString())).thenReturn(getDcTbls().get(0));
        when(mhaTblDao.queryByMhaName(Mockito.anyString())).thenReturn(null);
        when(mhaTblDao.insertWithReturnId(Mockito.any(MhaTblV2.class))).thenReturn(1L);
        when(mhaReplicationTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyLong())).thenReturn(null);
        when(mhaReplicationTblDao.insert(Mockito.any(MhaReplicationTbl.class))).thenReturn(1);
        when(consoleConfig.getAccountKmsTokenSwitch()).thenReturn(true);
        when(consoleConfig.getAccountKmsTokenSwitchV2()).thenReturn(true);
        when(consoleConfig.getAccountFromMetaSwitch()).thenReturn(true);
        when(consoleConfig.getDefaultMonitorAccountKmsToken()).thenReturn("token");
        when(consoleConfig.getDefaultReadAccountKmsToken()).thenReturn("token");
        when(consoleConfig.getDefaultWriteAccountKmsToken()).thenReturn("token");
        when(kmsService.getAccountInfo(Mockito.anyString())).thenReturn(new Account("root","root"));
        MachineDto masterInSrcMha = new MachineDto(3306,"ip1",true);
        MachineDto masterInDstMha = new MachineDto(3306,"ip2",true);
        when(accountService.mhaAccountV2ChangeAndRecord(Mockito.any(MhaTblV2.class),Mockito.anyString(),Mockito.anyInt())).thenReturn(true);
        


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
        when(buTblDao.queryByBuName(Mockito.anyString())).thenReturn(getBuTbl());
        when(dcTblDao.queryByDcName(Mockito.anyString())).thenReturn(getDcTbls().get(0));
        when(mhaTblDao.queryByMhaName(Mockito.anyString())).thenReturn(null);
        when(mhaTblDao.insertWithReturnId(Mockito.any(MhaTblV2.class))).thenReturn(1L);
        when(mhaReplicationTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyLong())).thenReturn(null);
        when(mhaReplicationTblDao.insert(Mockito.any(MhaReplicationTbl.class))).thenReturn(1);
        when(consoleConfig.getAccountKmsTokenSwitch()).thenReturn(true);
        when(consoleConfig.getAccountKmsTokenSwitchV2()).thenReturn(true);
        when(consoleConfig.getAccountFromMetaSwitch()).thenReturn(true);
        when(consoleConfig.getDefaultMonitorAccountKmsToken()).thenReturn("token");
        when(consoleConfig.getDefaultReadAccountKmsToken()).thenReturn("token");
        when(consoleConfig.getDefaultWriteAccountKmsToken()).thenReturn("token");
        // for ambiguous mha
        when(consoleConfig.getAllowAmbiguousMhaSwitch()).thenReturn(true);
        MachineTbl machineTbl = new MachineTbl();
        machineTbl.setMhaId(111L);
        when(machineTblDao.queryByIpPort(Mockito.anyString(), Mockito.eq(13306))).thenReturn(machineTbl);
        when(mhaTblDao.queryById(Mockito.eq(111L))).thenReturn(new MhaTblV2());
        
        when(kmsService.getAccountInfo(Mockito.anyString())).thenReturn(new Account("root","root"));
        MachineDto masterInSrcMha = new MachineDto(13306,"ip1",true);
        MachineDto masterInDstMha = new MachineDto(13307,"ip2",true);
        MachineDto slaveInDstMha = new MachineDto(13306,"ip3",false);

        when(accountService.mhaAccountV2ChangeAndRecord(Mockito.any(MhaTblV2.class),Mockito.anyString(),Mockito.anyInt())).thenReturn(true);

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


        when(consoleConfig.getAllowAmbiguousMhaSwitch()).thenReturn(false);
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

        when(mhaTblDao.queryByMhaName(Mockito.eq("srcMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        when(mhaTblDao.queryByMhaName(Mockito.eq("dstMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        when(mhaTblDao.update(Mockito.any(MhaTblV2.class))).thenReturn(1);

        when(replicatorGroupTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(null);
        when(replicatorGroupTblDao.insertWithReturnId(Mockito.any())).thenReturn(200L);

        when(applierGroupTblDao.queryByMhaReplicationId(Mockito.anyLong())).thenReturn(null);
        when(applierGroupTblDao.insertWithReturnId(Mockito.any())).thenReturn(200L);

        when(replicatorTblDao.queryByRGroupIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(new ArrayList<>());
        when(replicatorTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[1]);

        when(applierTblDao.queryByApplierGroupId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(new ArrayList<>());
        when(applierTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[1]);

        when(resourceTblDao.queryByIps(Mockito.anyList())).thenReturn(getResourceTbls());
        when(mhaReplicationTblDao.insertWithReturnId(Mockito.any())).thenReturn(200L);
        when(mhaReplicationTblDao.insert(Mockito.any(MhaReplicationTbl.class))).thenReturn(1);
        when(mhaReplicationTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyInt())).thenReturn(getMhaReplicationTbls().get(0));
        when(mhaReplicationTblDao.update(Mockito.any(MhaReplicationTbl.class))).thenReturn(1);
        Mockito.doNothing().when(metaProviderV2).scheduledTask();
        when(metaInfoService.getDrcReplicationConfig(Mockito.anyString(), Mockito.anyString())).thenReturn(new Drc());

        when(mhaDbMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(getMhaDbMappingTbls1());
        when(dbReplicationTblDao.queryByMappingIds(Mockito.anyList(), Mockito.anyList(), Mockito.anyInt())).thenReturn(getDbReplicationTbls());

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
        when(mhaTblDao.queryByMhaName(Mockito.eq("srcMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        when(mhaTblDao.queryByMhaName(Mockito.eq("dstMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));

        List<String> dbList = Lists.newArrayList("db200");
        List<String> tableList = Lists.newArrayList("db200.table");
        when(mhaDbMappingService.initMhaDbMappings(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(Pair.of(dbList, tableList));

        when(mhaDbMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(getMhaDbMappingTbls1());
        when(dbTblDao.queryByDbNames(Mockito.anyList())).thenReturn(getDbTbls());
        when(dbTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbTbls());
        when(dbReplicationTblDao.queryByMappingIds(Mockito.anyList(), Mockito.anyList(), Mockito.anyInt())).thenReturn(getDbReplicationTbls1());
        when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.anyString(), Mockito.anyString())).thenReturn(Lists.newArrayList("db200.table"));
        when(dbReplicationTblDao.batchInsertWithReturnId(Mockito.anyList())).thenReturn(getDbReplicationTbls1());
        when(consoleConfig.getCflBlackListAutoAddSwitch()).thenReturn(true);
        try {
            Mockito.doNothing().when(conflictLogService).addDbBlacklist(Mockito.anyString(), Mockito.eq(
                    CflBlacklistType.NEW_CONFIG),Mockito.any());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        when(mhaServiceV2.getRegion(Mockito.anyString())).thenReturn("region");
        when(replicationTableTblDao.queryByDbReplicationIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(new ArrayList<>());
        when(replicationTableTblDao.insert(Mockito.anyList())).thenReturn(new int[1]);
        when(replicationTableTblDao.update(Mockito.anyList())).thenReturn(new int[1]);
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

        when(dbReplicationTblDao.queryByIds(dbReplicationIds)).thenReturn(dbReplicationTbls0);
        List<MhaTblV2> mhaTblV2s = getMhaTblV2s();
        when(mhaTblDao.queryByMhaName(Mockito.eq("srcMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        when(mhaTblDao.queryByMhaName(Mockito.eq("dstMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        when(mhaDbMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(getMhaDbMappingTbls1());
        when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.anyString(), Mockito.anyString())).thenReturn(Lists.newArrayList("db200.table"));
        when(dbTblDao.queryByDbNames(Mockito.anyList())).thenReturn(getDbTbls());
        when(dbTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbTbls());
        when(dbReplicationTblDao.update(Mockito.anyList())).thenReturn(new int[1]);
        when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());
        when(rowsFilterTblV2Dao.queryById(Mockito.anyLong())).thenReturn(getRowsFilterTbl());
        when(columnFilterTblV2Dao.queryById(Mockito.anyLong())).thenReturn(getColumnsFilterTbl());
        when(mysqlServiceV2.getCommonColumnIn(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(Sets.newHashSet("udl", "uid", "column"));
        when(consoleConfig.getCflBlackListAutoAddSwitch()).thenReturn(false);

        when(mhaServiceV2.getRegion(Mockito.anyString())).thenReturn("region");
        when(replicationTableTblDao.queryByDbReplicationIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(new ArrayList<>());
        when(replicationTableTblDao.insert(Mockito.anyList())).thenReturn(new int[1]);
        when(replicationTableTblDao.update(Mockito.anyList())).thenReturn(new int[1]);


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
        when(mhaTblDao.queryByMhaName(Mockito.eq("srcMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        when(mhaTblDao.queryByMhaName(Mockito.eq("dstMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));

        when(mhaDbMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(getMhaDbMappingTbls1());
        when(dbReplicationTblDao.queryByMappingIds(Mockito.anyList(), Mockito.anyList(), Mockito.anyInt())).thenReturn(getDbReplicationTbls());
        when(dbTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbTbls());
        when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());

        List<DbReplicationView> result = drcBuildServiceV2.getDbReplicationView("srcMha", "dstMha");
        Assert.assertEquals(result.size(), 2);
    }

    @Test
    public void testDeleteDbReplications() throws Exception {
        when(dbReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbReplicationTbls());
        when(dbReplicationTblDao.batchUpdate(Mockito.anyList())).thenReturn(new int[1]);
        when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        when(dbReplicationFilterMappingTblDao.batchUpdate(Mockito.anyList())).thenReturn(new int[1]);

        drcBuildServiceV2.deleteDbReplications(Lists.newArrayList(200L, 201L));
        Mockito.verify(dbReplicationTblDao, Mockito.times(1)).batchUpdate(Mockito.any());
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.never()).batchUpdate(Mockito.any());
        Mockito.verify(mhaDbReplicationService, Mockito.times(1)).offlineMhaDbReplication(Mockito.anyList());
    }

    @Test
    public void testBuildColumnsFilter() throws Exception {
        ColumnsFilterCreateParam param = new ColumnsFilterCreateParam(Lists.newArrayList(200L, 201L), 0, Lists.newArrayList("column"));
        when(dbReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbReplicationTbls());
        when(columnFilterTblV2Dao.queryByMode(Mockito.anyInt())).thenReturn(new ArrayList<>());
        when(columnFilterTblV2Dao.insertReturnId(Mockito.any())).thenReturn(200L);
        when(dbReplicationFilterMappingTblDao.queryByDbReplicationId(Mockito.anyLong())).thenReturn(new ArrayList<>());

        drcBuildServiceV2.buildColumnsFilter(param);
        Mockito.verify(columnFilterTblV2Dao, Mockito.times(1)).insertReturnId(Mockito.any());
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.times(1)).batchInsert(Mockito.anyList());
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.never()).batchUpdate(Mockito.anyList());

    }

    @Test
    public void testGetColumnsConfigView() throws Exception {
        when(dbReplicationFilterMappingTblDao.queryByDbReplicationId(Mockito.anyLong())).thenReturn(getFilterMappings());
        when(columnFilterTblV2Dao.queryById(Mockito.anyLong())).thenReturn(getColumnsFilterTbl());

        ColumnsConfigView result = drcBuildServiceV2.getColumnsConfigView(200L);
        Assert.assertEquals(result.getColumns().size(), 1);
    }

    @Test
    public void testDeleteColumnsFilter() throws Exception {
        when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());
        when(dbReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbReplicationTbls());
        when(dbReplicationFilterMappingTblDao.batchUpdate(Mockito.anyList())).thenReturn(new int[1]);

        drcBuildServiceV2.deleteColumnsFilter(Lists.newArrayList(200L, 201L));
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.times(1)).batchUpdate(Mockito.anyList());
    }

    @Test
    public void testGetRowsConfigView() throws Exception {
        when(dbReplicationFilterMappingTblDao.queryByDbReplicationId(Mockito.anyLong())).thenReturn(getFilterMappings());
        when(rowsFilterTblV2Dao.queryById(Mockito.anyLong())).thenReturn(getRowsFilterTbl());

        RowsFilterConfigView result = drcBuildServiceV2.getRowsConfigView(200L);
        Assert.assertTrue(CollectionUtils.isEmpty(result.getColumns()));
        Assert.assertEquals(result.getUdlColumns().size(), 1);
        Assert.assertEquals(result.getMode(), 1);
    }

    @Test
    public void testDeleteRowsFilter() throws Exception {
        when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());
        when(dbReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbReplicationTbls());
        when(dbReplicationFilterMappingTblDao.batchUpdate(Mockito.anyList())).thenReturn(new int[1]);

        drcBuildServiceV2.deleteRowsFilter(Lists.newArrayList(200L, 201L));
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.times(1)).batchUpdate(Mockito.anyList());
    }

    @Test
    public void testBuildRowsFilter() throws Exception {
        RowsFilterCreateParam param = getRowsFilterCreateParam();
        when(dbReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbReplicationTbls());
        when(rowsFilterTblV2Dao.queryByMode(Mockito.anyInt())).thenReturn(new ArrayList<>());
        when(rowsFilterTblV2Dao.insertReturnId(Mockito.any())).thenReturn(200L);
        when(dbReplicationFilterMappingTblDao.queryByDbReplicationId(Mockito.anyLong())).thenReturn(new ArrayList<>());

        drcBuildServiceV2.buildRowsFilter(param);
        Mockito.verify(rowsFilterTblV2Dao, Mockito.times(1)).insertReturnId(Mockito.any());
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.times(1)).batchInsert(Mockito.anyList());
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.never()).batchUpdate(Mockito.anyList());
    }

    @Test
    public void testGetApplierGtid() throws Exception {
        List<MhaTblV2> mhaTblV2s = getMhaTblV2s();
        when(mhaTblDao.queryByMhaName(Mockito.eq("srcMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        when(mhaTblDao.queryByMhaName(Mockito.eq("dstMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        when(mhaReplicationTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyLong())).thenReturn(getMhaReplicationTbl());
        when(applierGroupTblDao.queryByMhaReplicationId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(getApplierGroupTblV2s().get(0));

        String result = drcBuildServiceV2.getApplierGtid("srcMha", "dstMha");
        Assert.assertEquals(result, getApplierGroupTblV2s().get(0).getGtidInit());
    }

    @Test
    public void testBuildMessengerMha() throws Exception {
        List<DcTbl> dcTbls = getDcTbls();
        when(dcTblDao.queryByDcName(Mockito.anyString())).thenReturn(dcTbls.get(0));
        when(buTblDao.queryByBuName(Mockito.anyString())).thenReturn(getBuTbl());
        when(mhaTblDao.queryByMhaName(Mockito.anyString())).thenReturn(getMhaTblV2());
        when(messengerGroupTblDao.upsertIfNotExist(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString())).thenReturn(1L);

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
        when(replicatorGroupTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(MockEntityBuilder.buildReplicatorGroupTbl(1L, 1L));
        when(replicatorTblDao.queryByRGroupIds(Mockito.anyList(), Mockito.eq(0))).thenReturn(Lists.newArrayList());
        List<ResourceView> resourceViews = MockEntityBuilder.buildResourceViews(2, ModuleEnum.REPLICATOR.getCode());
        when(resourceService.autoConfigureResource(Mockito.any(ResourceSelectParam.class))).thenReturn(resourceViews);
        when(mysqlServiceV2.getMhaExecutedGtid(Mockito.anyString())).thenReturn("gtid");
        when(metaInfoService.findAvailableApplierPort(Mockito.anyString())).thenReturn(8383);
        when(replicatorTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[]{1, 1});
        drcBuildServiceV2.autoConfigReplicatorsWithRealTimeGtid(MockEntityBuilder.buildMhaTblV2());
    }

    @Test
    public void testAutoConfigAppliersWithRealTimeGtid() throws Exception {
        when(mysqlServiceV2.getMhaExecutedGtid(Mockito.anyString())).thenReturn("gtid");
        when(applierGroupTblDao.update(Mockito.any(ApplierGroupTblV2.class))).thenReturn(1);
        when(mhaReplicationTblDao.update(Mockito.any(MhaReplicationTbl.class))).thenReturn(1);
        when(resourceService.handOffResource(Mockito.any(ResourceSelectParam.class))).thenReturn(MockEntityBuilder.buildResourceViews(2,
                ModuleEnum.APPLIER.getCode()));
        when(applierTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[]{1, 1});
        MhaTblV2 mha1 = MockEntityBuilder.buildMhaTblV2(1L, "mha1", 1L);
        MhaTblV2 mha2 = MockEntityBuilder.buildMhaTblV2(2L, "mha2", 2L);
        MhaReplicationTbl mhaReplicationTbl = MockEntityBuilder.buildMhaReplicationTbl(1L, mha1, mha2);
        ApplierGroupTblV2 applierGroupTblV2 = MockEntityBuilder.buildApplierGroupTbl(1L, mhaReplicationTbl);
        drcBuildServiceV2.autoConfigAppliersWithRealTimeGtid(mhaReplicationTbl, applierGroupTblV2, mha1, mha2);
    }

    @Test
    public void testAutoConfigMessengersWithRealTimeGtid() throws Exception {
        String gtid = "abc-zyn-test:1235";
        when(mysqlServiceV2.getMhaExecutedGtid(Mockito.anyString())).thenReturn(gtid);
        when(messengerGroupTblDao.queryByMhaId(Mockito.anyLong(), Mockito.eq(0))).thenReturn(MockEntityBuilder.buildMessengerGroupTbl(1L, 1L));
        when(messengerGroupTblDao.update(Mockito.any(MessengerGroupTbl.class))).thenReturn(1);
        when(resourceService.handOffResource(Mockito.any(ResourceSelectParam.class))).thenReturn(MockEntityBuilder.buildResourceViews(2,
                ModuleEnum.APPLIER.getCode()));
        when(messengerTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[]{1, 1});
        drcBuildServiceV2.autoConfigMessengersWithRealTimeGtid(MockEntityBuilder.buildMhaTblV2(),Mockito.anyBoolean());
        Mockito.verify(messengerTblDao,Mockito.times(1)).batchInsert(Mockito.anyList());
        Mockito.verify(messengerGroupTblDao,Mockito.times(1)).update((MessengerGroupTbl) Mockito.argThat(e->{
            MessengerGroupTbl group = (MessengerGroupTbl) e;
            return group.getGtidExecuted().equals(gtid);
        }));
    }

    @Test
    public void testAutoConfigMessengersWithRealTimeGtidM() throws Exception {
        String gtid = "abc-zyn-test:1235";
        when(mysqlServiceV2.getMhaExecutedGtid(Mockito.anyString())).thenReturn(gtid);
        when(messengerGroupTblDao.queryByMhaId(Mockito.anyLong(), Mockito.eq(0))).thenReturn(MockEntityBuilder.buildMessengerGroupTbl(1L, 1L));
        when(messengerGroupTblDao.update(Mockito.any(MessengerGroupTbl.class))).thenReturn(1);
        when(resourceService.handOffResource(Mockito.any(ResourceSelectParam.class))).thenReturn(MockEntityBuilder.buildResourceViews(2,
                ModuleEnum.APPLIER.getCode()));
        when(messengerTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[]{1, 1});
        drcBuildServiceV2.autoConfigMessengersWithRealTimeGtidM(MockEntityBuilder.buildMhaTblV2(),Mockito.anyBoolean());
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
        when(messengerGroupTblDao.queryByMhaId(Mockito.anyLong(), Mockito.eq(0))).thenReturn(value);
        when(messengerGroupTblDao.update(Mockito.any(MessengerGroupTbl.class))).thenReturn(1);
        MessengerTbl messengerTbl = new MessengerTbl();
        messengerTbl.setResourceId(1001L);
        messengerTbl.setId(101L);
        when(messengerTblDao.queryByGroupId(Mockito.eq(1L))).thenReturn(Lists.newArrayList(messengerTbl));
        ResourceTbl resourceTbl = new ResourceTbl();
        resourceTbl.setId(1001L);
        resourceTbl.setIp("10.12.13.14");
        when(resourceTblDao.queryByIds(Mockito.anyList())).thenReturn(Lists.newArrayList(resourceTbl));
        when(resourceService.handOffResource(Mockito.any(ResourceSelectParam.class))).thenReturn(MockEntityBuilder.buildResourceViews(2,
                ModuleEnum.APPLIER.getCode()));
        when(messengerTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[]{1, 1});
        drcBuildServiceV2.autoConfigMessenger(MockEntityBuilder.buildMhaTblV2(),null,false);
        Mockito.verify(messengerTblDao,Mockito.times(1)).batchInsert(Mockito.anyList());
        Mockito.verify(messengerTblDao,Mockito.times(1)).batchUpdate(Mockito.argThat(e->e.stream().allMatch(tbl-> Objects.equals(tbl.getDeleted(), BooleanEnum.TRUE.getCode()))));
        Mockito.verify(messengerGroupTblDao,Mockito.never()).update((MessengerGroupTbl) Mockito.anyObject());
    }

    @Test
    public void testAutoConfigMessengersM() throws Exception {
        String gtid = "abc-zyn-test:1235";
        MessengerGroupTbl value = MockEntityBuilder.buildMessengerGroupTbl(1L, 1L);
        value.setGtidExecuted(gtid);
        when(messengerGroupTblDao.queryByMhaId(Mockito.anyLong(), Mockito.eq(0))).thenReturn(value);
        when(messengerGroupTblDao.update(Mockito.any(MessengerGroupTbl.class))).thenReturn(1);
        MessengerTbl messengerTbl = new MessengerTbl();
        messengerTbl.setResourceId(1001L);
        messengerTbl.setId(101L);
        when(messengerTblDao.queryByGroupId(Mockito.eq(1L))).thenReturn(Lists.newArrayList(messengerTbl));
        ResourceTbl resourceTbl = new ResourceTbl();
        resourceTbl.setId(1001L);
        resourceTbl.setIp("10.12.13.14");
        when(resourceTblDao.queryByIds(Mockito.anyList())).thenReturn(Lists.newArrayList(resourceTbl));
        when(resourceService.handOffResource(Mockito.any(ResourceSelectParam.class))).thenReturn(MockEntityBuilder.buildResourceViews(2,
                ModuleEnum.APPLIER.getCode()));
        when(messengerTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[]{1, 1});
        drcBuildServiceV2.autoConfigMessengerM(MockEntityBuilder.buildMhaTblV2(),null,false);
        Mockito.verify(messengerTblDao,Mockito.times(1)).batchInsert(Mockito.anyList());
        Mockito.verify(messengerTblDao,Mockito.times(1)).batchUpdate(Mockito.argThat(e->e.stream().allMatch(tbl-> Objects.equals(tbl.getDeleted(), BooleanEnum.TRUE.getCode()))));
        Mockito.verify(messengerGroupTblDao,Mockito.never()).update((MessengerGroupTbl) Mockito.anyObject());
    }

    @Test
    public void testInitReplicationTables() throws Exception {
        when(mhaReplicationTblDao.queryAllExist()).thenReturn(PojoBuilder.getMhaReplicationTbls1());
        when(mhaTblDao.queryAllExist()).thenReturn(PojoBuilder.getMhaTblV2s());
        when(dcTblDao.queryAllExist()).thenReturn(PojoBuilder.getDcTbls());
        when(mhaDbMappingTblDao.queryAllExist()).thenReturn(PojoBuilder.getMhaDbMappingTbls2());
        when(dbTblDao.queryAllExist()).thenReturn(PojoBuilder.getDbTbls());
        when(dbReplicationTblDao.queryByMappingIds(Mockito.anyList(), Mockito.anyList(), Mockito.anyInt())).thenReturn(PojoBuilder.getDbReplicationTbls());
        when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.anyString(), Mockito.anyString())).thenReturn(Lists.newArrayList("db200.table1"));

        when(replicationTableTblDao.insert(Mockito.anyList())).thenReturn(new int[0]);
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
        
        
        when(mhaTblDao.queryByPk(Mockito.anyList())).thenReturn(Lists.newArrayList(mha1));
        when(dcTblDao.queryByPk(Mockito.anyList())).thenReturn(Lists.newArrayList(dc1));
        
        when(mysqlServiceV2.getMhaExecutedGtid(Mockito.anyString())).thenReturn("u1:1-22,u2:1-22");
        when(mhaReplicationTblDao.queryBySrcMhaId(Mockito.anyLong())).thenReturn(Lists.newArrayList());
        when(applierGroupTblDao.queryByMhaReplicationIds(Mockito.anyList())).thenReturn(Lists.newArrayList(applierGroupTblV2));
        when(mhaDbReplicationService.queryBySrcMha(Mockito.anyString())).thenReturn(Lists.newArrayList(mhaDbReplicationTbl));
        when(dbApplierGroupTblDao.queryByMhaDbReplicationIds(Mockito.anyList())).thenReturn(Lists.newArrayList(applierGroupTblV3));
        when(messengerGroupTblDao.queryByMhaId(Mockito.anyLong(),Mockito.anyInt())).thenReturn(messengerGroupTbl);
        
        
        when(applierGroupTblDao.update(Mockito.anyList())).thenReturn(new int[]{1});
        when(dbApplierGroupTblDao.update(Mockito.anyList())).thenReturn(new int[]{1});
        when(messengerGroupTblDao.update(Mockito.anyList())).thenReturn(new int[]{1});

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
        when(mhaTblDao.queryByMhaName(Mockito.anyString())).thenReturn(mha1);
        when(mhaServiceV2.getMhaReplicatorsV2(Mockito.anyString())).thenReturn(
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
        
        when(resourceService.getMhaAvailableResource(Mockito.anyString(),Mockito.anyInt())).thenReturn(
                Lists.newArrayList(
                        resourceView1,
                        resourceView2
                )
        );
        when(mysqlServiceV2.getMhaExecutedGtid(Mockito.anyString())).thenReturn("gtidInit");
        when(replicatorTblDao.update(Mockito.any(ReplicatorTbl.class))).thenReturn(1);

        int i = drcBuildServiceV2.isolationMigrateReplicator(Lists.newArrayList("mha1"), true, "tag3", null);
        Assert.assertEquals(1,i);


        when(mhaServiceV2.getMhaReplicatorsV2(Mockito.anyString())).thenReturn(
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
        when(mhaReplicationTblDao.queryByDstMhaId(Mockito.anyLong())).thenReturn(
                Lists.newArrayList(
                        MockEntityBuilder.buildMhaReplicationTbl(
                            1L,
                                mha1,
                                mha2
                        )
                )
        );
        when(mhaTblDao.queryByMhaName(Mockito.anyString())).thenReturn(mha2);
        when(mhaTblDao.queryByPk(Mockito.anyLong())).thenReturn(mha1);
        when(mhaTblDao.queryByMhaName(Mockito.anyString(),Mockito.anyInt())).thenReturn(mha2);
        when(messengerGroupTblDao.queryByMhaId(Mockito.anyLong(),Mockito.anyInt())).thenReturn(MockEntityBuilder.buildMessengerGroupTbl(2L,2L));
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
        
        when(resourceTblDao.queryBy(Mockito.any())).thenReturn(Lists.newArrayList(resource1,resource2));
        when(metaProviderV2.getDrc()).thenReturn(newDrc);

        Pair<Boolean, String> checkRes = drcBuildServiceV2.checkIsoMigrateStatus(Lists.newArrayList("mha1", "mha2"), "tag");
        Assert.assertFalse(checkRes.getKey());

        resource2.setIp("testip");
        checkRes = drcBuildServiceV2.checkIsoMigrateStatus(Lists.newArrayList("mha1", "mha2"), "tag");
        Assert.assertTrue(checkRes.getKey());
    }
    
    @Test
    public void testSyncMhaInfoFormDbaApi() throws SQLException {
        when(mhaTblDao.queryByMhaName(anyString(),anyInt())).thenAnswer(
                e -> {
                    Object[] arguments = e.getArguments();
                    return Objects.equals(arguments[0],"oldMha") ? oldMha() : null;
                }
        );
        when(dbaApiService.getClusterMembersInfo(eq("newMha"))).thenReturn(dbaClusterInfoResponse());
        when(consoleConfig.getDbaDc2DrcDcMap()).thenReturn(Map.of("dba_dc1","drc_dc1"));
        when(dcTblDao.queryByDcName(eq("drc_dc1"))).thenReturn(dcTbl());
        when(mhaTblDao.insertWithReturnId(any(MhaTblV2.class))).thenReturn(2L);
        when(machineTblDao.queryByIpPort(anyString(),anyInt())).thenReturn(null);
        when(accountService.mhaAccountV2ChangeAndRecord(any(MhaTblV2.class),anyString(),anyInt())).thenReturn(true);
        when(machineTblDao.batchInsert(anyList())).thenReturn(new int[]{1});
        // case1 init new without oldMha
        MhaTblV2 newMha = drcBuildServiceV2.syncMhaInfoFormDbaApi("newMha", null);
        verify(accountService,times(1)).mhaAccountV2ChangeAndRecord(any(MhaTblV2.class),anyString(),anyInt());
        Assert.assertNull(newMha.getMonitorUserV2());
        // case2 init newMha copy from oldMha
        newMha = drcBuildServiceV2.syncMhaInfoFormDbaApi("newMha", "oldMha");
        verify(accountService,times(1)).mhaAccountV2ChangeAndRecord(any(MhaTblV2.class),anyString(),anyInt());
        Assert.assertEquals("monitorUserV2",newMha.getMonitorUserV2());
        
        
    }

    private DcTbl dcTbl() {
        DcTbl dcTbl = new DcTbl();
        dcTbl.setId(1L);
        dcTbl.setDcName("drc_dc1");
        return dcTbl;
    }

    private DbaClusterInfoResponse dbaClusterInfoResponse() {
        DbaClusterInfoResponse value = new DbaClusterInfoResponse();
        Data data = new Data();
        List<MemberInfo> memberInfos = org.assertj.core.util.Lists.newArrayList();
        MemberInfo memberInfo = new MemberInfo();
        memberInfo.setService_ip("ip1");
        memberInfo.setDns_port(3306);
        memberInfo.setRole("master");
        memberInfo.setMachine_located_short("dba_dc1");
        memberInfos.add(memberInfo);
        data.setMemberlist(memberInfos);
        value.setData(data);
        return value;
    }
    
    private MhaTblV2 oldMha() {
        MhaTblV2 oldMha = new MhaTblV2();
        oldMha.setId(1L);
        oldMha.setMhaName("oldMha");
        oldMha.setDcId(1L);
        oldMha.setBuId(1L);
        oldMha.setTag("tag1");
        oldMha.setReadUserV2("readUserV2");
        oldMha.setReadPasswordTokenV2("readPasswordTokenV2");
        oldMha.setWriteUserV2("writeUserV2");
        oldMha.setWritePasswordTokenV2("writePasswordTokenV2");
        oldMha.setMonitorUserV2("monitorUserV2");
        oldMha.setMonitorPasswordTokenV2("monitorPasswordTokenV2");
        oldMha.setMonitorSwitch(BooleanEnum.TRUE.getCode());
        oldMha.setDeleted(BooleanEnum.FALSE.getCode());
        return oldMha;
    }
    

}
