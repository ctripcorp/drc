package com.ctrip.framework.drc.console.service.log;


import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.core.service.email.Email;
import com.ctrip.framework.drc.core.service.email.EmailResponse;
import com.ctrip.framework.drc.core.service.email.EmailService;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallConflictCount;
import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallConflictCount.Metric;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import javax.validation.groups.Default;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class ConflictAlarmTest {
    
    @InjectMocks
    private ConflictAlarm conflictAlarm;

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

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
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
        
        conflictAlarm.checkConflict();
        Mockito.verify(emailService, Mockito.times(4)).sendEmail(Mockito.any(Email.class));
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