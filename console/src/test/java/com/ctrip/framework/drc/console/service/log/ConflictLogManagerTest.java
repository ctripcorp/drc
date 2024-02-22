package com.ctrip.framework.drc.console.service.log;


import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.log.ConflictDbBlackListTblDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictDbBlackListTbl;
import com.ctrip.framework.drc.console.enums.log.CflBlacklistType;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.core.service.email.Email;
import com.ctrip.framework.drc.core.service.email.EmailResponse;
import com.ctrip.framework.drc.core.service.email.EmailService;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallConflictCount;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import java.sql.Timestamp;
import java.util.List;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

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
    @Mock
    private DbaApiService dbaApiService;
    @Mock
    private DbBlacklistCache dbBlacklistCache;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }
    
    @Test
    public void testSchedule() throws Throwable {
        // mock check conflictCount
        Mockito.when(domainConfig.getTrafficFromHickWall()).thenReturn("http://hickwall.com");
        Mockito.when(domainConfig.getOpsAccessToken()).thenReturn("opsAccessToken");
        Mockito.when(domainConfig.getTrafficFromHickWallFat()).thenReturn("http://fat.hickwall.com");
        Mockito.when(domainConfig.getOpsAccessTokenFat()).thenReturn("fatOpsAccessToken");
        Mockito.when(domainConfig.getConflictAlarmThresholdCommitTrx()).thenReturn(100L);
        Mockito.when(domainConfig.getConflictAlarmThresholdCommitRow()).thenReturn(100L);
        Mockito.when(domainConfig.getConflictAlarmThresholdRollbackTrx()).thenReturn(100L);
        Mockito.when(domainConfig.getConflictAlarmThresholdRollbackRow()).thenReturn(100L);
        Mockito.when(domainConfig.getConflictAlarmSendEmailSwitch()).thenReturn(true);
        Mockito.when(domainConfig.getConflictAlarmSendDBOwnerSwitch()).thenReturn(true);
        Mockito.when(domainConfig.getConflictAlarmCCEmails()).thenReturn(Lists.newArrayList("ccEmail1","ccEmail2"));
        Mockito.when(domainConfig.getConflictAlarmHickwallUrl()).thenReturn("http://drc.hickwall.com");
        Mockito.when(domainConfig.getConflictAlarmDrcUrl()).thenReturn("http://drc.trip.com");
        Mockito.when(dbaApiService.everUserTraffic(Mockito.anyString(),anyString(),anyString(),anyLong(),anyLong(),anyBoolean())).thenReturn(true);

        Mockito.when(conflictLogService.isInBlackListWithCache(Mockito.eq("blackDb"), Mockito.anyString())).thenReturn(true);
        Mockito.when(conflictLogService.isInBlackListWithCache(Mockito.eq("notBlackDb"), Mockito.anyString())).thenReturn(false);
        Mockito.when(mhaServiceV2.getRegion(Mockito.anyString())).thenReturn("region");
        Mockito.when(dbTblDao.queryByDbNames(Mockito.anyList())).thenReturn(getDbTbls());
        Mockito.when(emailService.sendEmail(Mockito.any(Email.class))).thenReturn(new EmailResponse());

        Mockito.when(opsApiService.getConflictCount(Mockito.anyString(), Mockito.anyString(),
                        Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyInt()))
                .thenReturn(getConflictCounts());
        Mockito.when(domainConfig.getConflictAlarmLimitPerHour()).thenReturn(2);

        // mock clear blacklist
        List<ConflictDbBlackListTbl> cflBlackLists = Lists.newArrayList();
        cflBlackLists.addAll(getCflLogBlackListTbls(CflBlacklistType.USER));
        cflBlackLists.addAll(getCflLogBlackListTbls(CflBlacklistType.NEW_CONFIG,true));
        cflBlackLists.addAll(getCflLogBlackListTbls(CflBlacklistType.DBA_JOB,true));
        cflBlackLists.addAll(getCflLogBlackListTbls(CflBlacklistType.ALARM_HOTSPOT,true));
        cflBlackLists.addAll(getCflLogBlackListTbls(CflBlacklistType.NO_USER_TRAFFIC,false)); // not expired
        
        when(cflLogBlackListTblDao.queryAllExist()).thenReturn(cflBlackLists);
        when(domainConfig.getBlacklistExpirationHour(any())).thenReturn(5);
        when(cflLogBlackListTblDao.update(any(ConflictDbBlackListTbl.class))).thenReturn(1);
        
        when(domainConfig.getBlacklistClearSwitch(any())).thenReturn(true);
        when(cflLogBlackListTblDao.batchDelete(anyList())).thenReturn(null);
        doNothing().when(dbBlacklistCache).refresh(true);
        
        // mock add alarm hotspot table to blacklist
        Mockito.when(domainConfig.getBlacklistAlarmHotspotThreshold()).thenReturn(1L);
        Mockito.doNothing().when(conflictLogService).addDbBlacklist(Mockito.anyString(), Mockito.any(CflBlacklistType.class),Mockito.any());
        
        // mock schedule status
        Mockito.when(consoleConfig.isCenterRegion()).thenReturn(true);
        conflictLogManager.setRegionLeader(true);
        conflictLogManager.minuteCount = 60 * 24 -1;
        
        conflictLogManager.scheduledTask();
        verify(emailService, Mockito.times(2)).sendEmail(Mockito.any(Email.class));
        verify(cflLogBlackListTblDao, Mockito.times(3)).batchDelete(Mockito.anyList());
        Assert.assertEquals(0,conflictLogManager.minuteCount);
        Assert.assertEquals(0,conflictLogManager.tableAlarmCountMap.size());
        Assert.assertEquals(0,conflictLogManager.tableAlarmCountHourlyMap.size());
    }
    

    private List<ConflictDbBlackListTbl> getCflLogBlackListTbls(CflBlacklistType type,boolean isExpired) {
        ConflictDbBlackListTbl blackListTbl = new ConflictDbBlackListTbl();
        blackListTbl.setType(type.getCode());
        blackListTbl.setDbFilter("db1\\..*");
        blackListTbl.setExpirationTime(isExpired ? new Timestamp(System.currentTimeMillis()) : new Timestamp(System.currentTimeMillis() + 1000 * 60 * 60));
        blackListTbl.setDatachangeLasttime(new Timestamp(System.currentTimeMillis() - 1000 * 60 * 60 * 5));
        return Lists.newArrayList(blackListTbl);
    }

    private List<ConflictDbBlackListTbl> getCflLogBlackListTbls(CflBlacklistType type) {
        ConflictDbBlackListTbl blackListTbl = new ConflictDbBlackListTbl();
        blackListTbl.setType(type.getCode());
        blackListTbl.setDbFilter("db1\\..*");
        blackListTbl.setDatachangeLasttime(new Timestamp(System.currentTimeMillis() - 1000 * 60 * 60 * 5));
        return Lists.newArrayList(blackListTbl);
    }


    private List<DbTbl> getDbTbls() {
        DbTbl dbTbl = new DbTbl();
        dbTbl.setDbOwner("dbOwner1");
        dbTbl.setEmailGroup("test@trip.com");
        return Lists.newArrayList(dbTbl);
    }
    
    private List<HickWallConflictCount> getConflictCounts() {
        String jsonResult = "[{\"metric\":{\"db\":\"blackDb\",\"destMha\":\"fatbbzxy\",\"srcMha\":\"fatbbzxh\",\"table\":\"tabble\"},\"values\":[[1701071020,\"2\"],[1701071080,\"0\"],[1701071140,\"1000\"]]},{\"metric\":{\"db\":\"notBlackDb\",\"destMha\":\"zyn_test_1\",\"srcMha\":\"zyn_test_2\",\"table\":\"test\"},\"values\":[[1701070840,\"0\"],[1701070900,\"0\"],[1701070960,\"0\"],[1701071020,\"0\"],[1701071080,\"0\"],[1701071140,\"1000\"]]}]";
        return JsonUtils.fromJsonToList(jsonResult, HickWallConflictCount.class);
    }


    
}