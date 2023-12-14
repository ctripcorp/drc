package com.ctrip.framework.drc.console.service.log;


import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.log.ConflictDbBlackListTblDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictDbBlackListTbl;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam.MigrateMhaInfo;
import com.ctrip.framework.drc.console.enums.log.LogBlackListType;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.core.service.email.Email;
import com.ctrip.framework.drc.core.service.email.EmailResponse;
import com.ctrip.framework.drc.core.service.email.EmailService;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallConflictCount;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class ConflictLogManagerTest {
    
    @InjectMocks
    private ConflictLogManager conflictLogManager;

    @Mock
    private ConflictLogService conflictLogService;
    @Mock
    private DbTblDao dbTblDao;
    @Mock
    private MhaServiceV2 mhaServiceV2;
    @Mock
    private DomainConfig domainConfig;
    @Mock
    private OPSApiService opsApiService;
    @Mock
    private EmailService emailService;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private ConflictDbBlackListTblDao cflLogBlackListTblDao;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }
    
    @Test
    public void test() {
        DbMigrationParam dbMigrationParam = new DbMigrationParam();
        MigrateMhaInfo oldMha = new MigrateMhaInfo();
        oldMha.setName("fat-bbz-pub-13");
        oldMha.setMasterIp("10.21.6.216");
        oldMha.setMasterPort(55111);
        dbMigrationParam.setOldMha(oldMha);
        
        MigrateMhaInfo newMha = new MigrateMhaInfo();
        newMha.setName("fat-bbz-pub-14");
        newMha.setMasterIp("testip");
        newMha.setMasterPort(55111);
        dbMigrationParam.setNewMha(newMha);
        
        dbMigrationParam.setDbs(Lists.newArrayList("bbzsoftivrdb"));
        dbMigrationParam.setOperator("test");
        String s = JsonUtils.toJson(dbMigrationParam);
        System.out.println(s);
    }
    

    @Test
    public void testCheckConflict() throws IOException, SQLException {
        Mockito.when(domainConfig.getTrafficFromHickWall()).thenReturn("http://hickwall.com");
        Mockito.when(domainConfig.getOpsAccessToken()).thenReturn("opsAccessToken");
        Mockito.when(domainConfig.getTrafficFromHickWallFat()).thenReturn("http://fat.hickwall.com");
        Mockito.when(domainConfig.getOpsAccessTokenFat()).thenReturn("fatOpsAccessToken");
        Mockito.when(domainConfig.getConflictCommitTrxThreshold()).thenReturn(100L);
        Mockito.when(domainConfig.getConflictCommitRowThreshold()).thenReturn(100L);
        Mockito.when(domainConfig.getConflictRollbackTrxThreshold()).thenReturn(100L);
        Mockito.when(domainConfig.getConflictRollbackRowThreshold()).thenReturn(100L);
        Mockito.when(domainConfig.getSendConflictAlarmEmailSwitch()).thenReturn(true);
        Mockito.when(domainConfig.getSendDbOwnerConflictEmailToSwitch()).thenReturn(true);
        Mockito.when(domainConfig.getConflictAlarmCCEmails()).thenReturn(Lists.newArrayList("ccEmail1","ccEmail2"));
        Mockito.when(domainConfig.getDrcHickwallMonitorUrl()).thenReturn("http://drc.hickwall.com");
        Mockito.when(domainConfig.getDrcConflictHandleUrl()).thenReturn("http://drc.trip.com");
        
        Mockito.when(conflictLogService.isInBlackListWithCache(Mockito.eq("blackDb"), Mockito.anyString())).thenReturn(false);
        Mockito.when(conflictLogService.isInBlackListWithCache(Mockito.eq("notBlackDb"), Mockito.anyString())).thenReturn(true);
        Mockito.when(mhaServiceV2.getRegion(Mockito.anyString())).thenReturn("region");
        Mockito.when(dbTblDao.queryByDbNames(Mockito.anyList())).thenReturn(getDbTbls());
        Mockito.when(emailService.sendEmail(Mockito.any(Email.class))).thenReturn(new EmailResponse());
        
        Mockito.when(opsApiService.getConflictCount(Mockito.anyString(), Mockito.anyString(), 
                Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyInt()))
                .thenReturn(getConflictCounts());
        Mockito.when(domainConfig.getSendConflictAlarmEmailSwitch()).thenReturn(true);
        Mockito.when(domainConfig.getConflictAlarmTimesPerHour()).thenReturn(2);
        
        conflictLogManager.checkConflict();
        Mockito.verify(emailService, Mockito.times(4)).sendEmail(Mockito.any(Email.class));
    }

    @Test
    public void testClearBlackListAddedAutomatically() throws SQLException {
        Mockito.when(consoleConfig.getCflBlackListAutoAddSwitch()).thenReturn(true);
        Mockito.when(cflLogBlackListTblDao.queryByType(LogBlackListType.AUTO.getCode())).thenReturn(getCflLogBlackListTbls(LogBlackListType.AUTO));
        Mockito.when(domainConfig.getCflBlackListExpirationHour()).thenReturn(5);
        Mockito.when(cflLogBlackListTblDao.batchDelete(Mockito.anyList())).thenReturn(new int[]{1});
        conflictLogManager.clearBlackListAddedAutomatically(LogBlackListType.AUTO);
        Mockito.verify(cflLogBlackListTblDao, Mockito.times(1)).batchDelete(Mockito.anyList());

        Mockito.when(consoleConfig.getDBACflBlackListClearSwitch()).thenReturn(true);
        Mockito.when(cflLogBlackListTblDao.queryByType(LogBlackListType.DBA.getCode())).thenReturn(getCflLogBlackListTbls(LogBlackListType.DBA));
        Mockito.when(domainConfig.getDBACflBlackListExpirationHour()).thenReturn(5);
        Mockito.when(cflLogBlackListTblDao.batchDelete(Mockito.anyList())).thenReturn(new int[]{1});
        conflictLogManager.clearBlackListAddedAutomatically(LogBlackListType.DBA);
        Mockito.verify(cflLogBlackListTblDao, Mockito.times(2)).batchDelete(Mockito.anyList());
        
    }

    private List<ConflictDbBlackListTbl> getCflLogBlackListTbls(LogBlackListType type) {
        ConflictDbBlackListTbl blackListTbl = new ConflictDbBlackListTbl();
        blackListTbl.setType(type.getCode());
        blackListTbl.setDbFilter("db1\\..*");
        blackListTbl.setDatachangeLasttime(new Timestamp(System.currentTimeMillis() - 1000 * 60 * 60 * 5));
        return Lists.newArrayList(blackListTbl);
    }


    private List<DbTbl> getDbTbls() {
        DbTbl dbTbl = new DbTbl();
        dbTbl.setDbOwner("dbOwner1");
        return Lists.newArrayList(dbTbl);
    }
    
    private List<HickWallConflictCount> getConflictCounts() {
        String jsonResult = "[{\"metric\":{\"db\":\"blackDb\",\"destMha\":\"fatbbzxy\",\"srcMha\":\"fatbbzxh\",\"table\":\"tabble\"},\"values\":[[1701071020,\"2\"],[1701071080,\"0\"],[1701071140,\"1000\"]]},{\"metric\":{\"db\":\"notBlackDb\",\"destMha\":\"zyn_test_1\",\"srcMha\":\"zyn_test_2\",\"table\":\"test\"},\"values\":[[1701070840,\"0\"],[1701070900,\"0\"],[1701070960,\"0\"],[1701071020,\"0\"],[1701071080,\"0\"],[1701071140,\"1000\"]]}]";
        return JsonUtils.fromJsonToList(jsonResult, HickWallConflictCount.class);
    }


    
}