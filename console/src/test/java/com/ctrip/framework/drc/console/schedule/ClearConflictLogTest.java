package com.ctrip.framework.drc.console.schedule;

import com.ctrip.framework.drc.console.dao.ConflictLogDao;
import com.ctrip.framework.drc.console.dao.entity.ConflictLog;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.platform.dal.dao.DalQueryDao;
import com.ctrip.platform.dal.dao.DalRowMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.concurrent.ScheduledExecutorService;


/**
 * Created by jixinwang on 2020/11/15
 */
public class ClearConflictLogTest {

    @InjectMocks
    private ClearConflictLog clearConflictLog;

    @Mock
    private ConflictLogDao conflictLogDao;

    @Mock
    private DalQueryDao queryDao;

    @Mock
    private MonitorTableSourceProvider configService;

    @Mock
    private ScheduledExecutorService conflictLogClearScheduledExecutorService;

    @Mock
    private DalRowMapper<ConflictLog> conflictLogDalRowMapper;


    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testInitSchedule() {
        clearConflictLog.scheduledTask();
    }

    @Test
    public void testDeleteConflictLog() throws SQLException {
        Mockito.when(conflictLogDao.count()).thenReturn(101);
        clearConflictLog.deleteConflictLog();
    }

    @Test
    public void testGetPastDate() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.DAY_OF_YEAR, calendar.get(Calendar.DAY_OF_YEAR) - 7);
        Date today = calendar.getTime();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String expect = format.format(today);
        String date = clearConflictLog.getPastDate(7);
        Assert.assertEquals(expect + " 00:00:00", date);
    }

    @Test
    public void getBatchConflictLog() throws SQLException {
        clearConflictLog.getBatchConflictLog("test sql");
    }

}
