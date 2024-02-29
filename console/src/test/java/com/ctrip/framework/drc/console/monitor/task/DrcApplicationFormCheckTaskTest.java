package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.v2.ApplicationApprovalTblDao;
import com.ctrip.framework.drc.console.dao.v2.ApplicationFormTblDao;
import com.ctrip.framework.drc.console.service.v2.DrcApplicationService;
import com.ctrip.framework.drc.console.service.v2.PojoBuilder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Created by dengquanliang
 * 2024/2/29 17:13
 */
public class DrcApplicationFormCheckTaskTest {

    @InjectMocks
    private DrcApplicationFormCheckTask task;
    @Mock
    private ApplicationFormTblDao applicationFormTblDao;
    @Mock
    private ApplicationApprovalTblDao applicationApprovalTblDao;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private DrcApplicationService drcApplicationService;
    @Mock
    private Reporter reporter;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testScheduledTask () throws Exception {
        Mockito.when(applicationFormTblDao.queryByIsSentEmail(Mockito.anyInt())).thenReturn(Lists.newArrayList(PojoBuilder.buildApplicationFormTbl()));
        Mockito.when(applicationApprovalTblDao.queryByApplicationFormIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(Lists.newArrayList(PojoBuilder.buildApplicationApprovalTbl1()));
        Mockito.when(drcApplicationService.sendEmail(Mockito.anyLong())).thenReturn(true);
        task.check();
        Mockito.verify(reporter, Mockito.never()).reportResetCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.anyString());

        Mockito.when(drcApplicationService.sendEmail(Mockito.anyLong())).thenReturn(false);
        task.check();
        Mockito.verify(reporter, Mockito.times(1)).reportResetCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.anyString());
    }

}