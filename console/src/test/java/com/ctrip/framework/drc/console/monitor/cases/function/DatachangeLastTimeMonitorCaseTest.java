package com.ctrip.framework.drc.console.monitor.cases.function;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.enums.ActionEnum;
import com.ctrip.framework.drc.console.mock.LocalMasterMySQLEndpointObservable;
import com.ctrip.framework.drc.console.monitor.delay.DelayMap;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.service.impl.HealthServiceImpl;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Triple;

import java.sql.SQLException;
import java.sql.Timestamp;

import static com.ctrip.framework.drc.console.monitor.cases.function.DatachangeLastTimeMonitorCase.FAIL_THRESHOLD;
import static com.ctrip.framework.drc.console.monitor.cases.function.DatachangeLastTimeMonitorCase.MONITOR_ROUND_RESET_POINT;
import static com.ctrip.framework.drc.console.utils.UTConstants.*;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-27
 */
public class DatachangeLastTimeMonitorCaseTest extends AbstractTest {

    Logger logger = LoggerFactory.getLogger(getClass());

    private DelayMap delayMap = DelayMap.getInstance();

    @InjectMocks
    private DatachangeLastTimeMonitorCase monitorCase = new DatachangeLastTimeMonitorCase();

    @Mock
    private HealthServiceImpl healthService;

    private static final String INSERT_DELAYMONITOR = "INSERT INTO `drcmonitordb`.`delaymonitor`(`src_ip`, `dest_ip`, `datachange_lasttime`) VALUES('%s', '%s', '%s');";

    private static final String UPDATE_DELAYMONITOR = "UPDATE `drcmonitordb`.`delaymonitor` SET `datachange_lasttime` = '%s' WHERE `src_ip`='%s';";

    private static final String MHA = "testMha";

    private static final String TARGET_MHA = "testTargetMha";

    private static final String TARGET_DC = "testdc";

    private Endpoint masterDb = new DefaultEndPoint("127.0.0.1", 12345, "root", null);

    private WriteSqlOperatorWrapper writeSqlOperatorWrapper;

    private static final double DELTA = 0.1;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        writeSqlOperatorWrapper = monitorCase.getSqlOperatorWrapper(masterDb);
        monitorCase.localDcName = DC1;
    }

    @Test
    public void testObserve() {
        Assert.assertEquals(0, monitorCase.masterMySQLEndpointMap.size());

        monitorCase.update(new Triple<>(META_KEY3, MYSQL_ENDPOINT1_MHA1DC1, ActionEnum.ADD), new LocalMasterMySQLEndpointObservable());
        Assert.assertEquals(0, monitorCase.masterMySQLEndpointMap.size());

        monitorCase.update(new Triple<>(META_KEY1, MYSQL_ENDPOINT1_MHA1DC1, ActionEnum.ADD), new LocalMasterMySQLEndpointObservable());
        Assert.assertEquals(1, monitorCase.masterMySQLEndpointMap.size());
        Assert.assertEquals(MYSQL_ENDPOINT1_MHA1DC1, monitorCase.masterMySQLEndpointMap.get(META_KEY1));

        monitorCase.update(new Triple<>(META_KEY1, MYSQL_ENDPOINT1_MHA2DC1, ActionEnum.UPDATE), new LocalMasterMySQLEndpointObservable());
        Assert.assertEquals(1, monitorCase.masterMySQLEndpointMap.size());
        Assert.assertEquals(MYSQL_ENDPOINT1_MHA2DC1, monitorCase.masterMySQLEndpointMap.get(META_KEY1));

        monitorCase.update(new Triple<>(META_KEY1, MYSQL_ENDPOINT1_MHA2DC1, ActionEnum.DELETE), new LocalMasterMySQLEndpointObservable());
        Assert.assertEquals(0, monitorCase.masterMySQLEndpointMap.size());
    }

    @Test
    public void testHandleMonitorRoundCount() {
        for(int i = 0; i < MONITOR_ROUND_RESET_POINT; ++i) {
            monitorCase.handleMonitorRoundCount();
            Assert.assertEquals(i+1, monitorCase.getMonitorRoundCount());
        }
        monitorCase.handleMonitorRoundCount();
        Assert.assertEquals(0, monitorCase.getMonitorRoundCount());
    }

    @Test
    public void testUpdateDrcDelay() throws SQLException, InterruptedException {
        // init
        long timestampInMillis1 = System.currentTimeMillis();
        Timestamp timestamp1 = new Timestamp(timestampInMillis1);
        String sql = String.format(INSERT_DELAYMONITOR, TARGET_DC, TARGET_DC, timestamp1);
        GeneralSingleExecution execution = new GeneralSingleExecution(sql);
        writeSqlOperatorWrapper.insert(execution);

        DelayMap.DrcDirection drcDirection = new DelayMap.DrcDirection(TARGET_MHA, MHA);

        Thread.sleep(10);
        Assert.assertEquals(-1, delayMap.size(drcDirection));

        Mockito.when(healthService.isLocalDcUpdating(Mockito.anyString())).thenReturn(false);
        Mockito.when(healthService.isTargetDcUpdating(Mockito.anyString())).thenReturn(false);
        monitorCase.updateDrcDelay(MHA, masterDb, TARGET_DC, TARGET_MHA);
        Assert.assertEquals(-1, delayMap.size(drcDirection));
        double avg1 = delayMap.avg(drcDirection);// initialize while call avg
        logger.info("avg1: {}", avg1);
        Assert.assertEquals(0.0, avg1, DELTA);

        Thread.sleep(10);
        monitorCase.updateDrcDelay(MHA, masterDb, TARGET_DC, TARGET_MHA);
        Assert.assertEquals(0, delayMap.size(drcDirection));
        double avg2 = delayMap.avg(drcDirection);
        logger.info("avg2: {}", avg2);
        Assert.assertEquals(avg1, avg2, DELTA);

        Mockito.when(healthService.isLocalDcUpdating(Mockito.anyString())).thenReturn(true);
        Mockito.when(healthService.isTargetDcUpdating(Mockito.anyString())).thenReturn(true);
        monitorCase.updateDrcDelay(MHA, masterDb, TARGET_DC, TARGET_MHA);
        Assert.assertEquals(0, delayMap.size(drcDirection));
        avg1 = delayMap.avg(drcDirection);
        logger.info("avg1: {}", avg1);
        Assert.assertEquals(0, avg1, DELTA);

        timestampInMillis1 = System.currentTimeMillis();
        timestamp1 = new Timestamp(timestampInMillis1-5001);
        sql = String.format(UPDATE_DELAYMONITOR, timestamp1, TARGET_DC);
        execution = new GeneralSingleExecution(sql);
        writeSqlOperatorWrapper.update(execution);
        Thread.sleep(10);
        monitorCase.monitorRoundCount = MONITOR_ROUND_RESET_POINT;
        monitorCase.updateDrcDelay(MHA, masterDb, TARGET_DC, TARGET_MHA);
        Assert.assertEquals(0, delayMap.size(drcDirection));
        avg1 = delayMap.avg(drcDirection);
        logger.info("avg1: {}", avg1);
        Assert.assertEquals(0, avg1, DELTA);

        timestampInMillis1 = System.currentTimeMillis();
        timestamp1 = new Timestamp(timestampInMillis1);
        sql = String.format(UPDATE_DELAYMONITOR, timestamp1, TARGET_DC);
        execution = new GeneralSingleExecution(sql);
        writeSqlOperatorWrapper.update(execution);
        Thread.sleep(10);
        monitorCase.updateDrcDelay(MHA, masterDb, TARGET_DC, TARGET_MHA);
        Assert.assertEquals(1, delayMap.size(drcDirection));
        avg1 = delayMap.avg(drcDirection);
        logger.info("avg1: {}", avg1);
        Assert.assertNotEquals(0, avg1, DELTA);

        Thread.sleep(10);
        monitorCase.updateDrcDelay(MHA, masterDb, TARGET_DC, TARGET_MHA);
        Assert.assertEquals(2, delayMap.size(drcDirection));
        avg2 = delayMap.avg(drcDirection);
        logger.info("avg2: {}", avg2);
        Assert.assertNotEquals(avg1, avg2, DELTA);
    }

    @Test
    public void testHandleConsoleUpdating() {
        DelayMap.DrcDirection drcDirection = new DelayMap.DrcDirection(MHA, TARGET_MHA);
        Mockito.when(healthService.isLocalDcUpdating(Mockito.anyString())).thenReturn(true);
        Mockito.when(healthService.isTargetDcUpdating(Mockito.anyString())).thenReturn(true);
        monitorCase.handleConsoleUpdating(drcDirection);
        Assert.assertEquals(0, monitorCase.getFailCount().get(drcDirection).intValue());
        Assert.assertEquals(-1, delayMap.size(drcDirection));

        delayMap.put(drcDirection, 1);
        Assert.assertEquals(1, delayMap.size(drcDirection));
        monitorCase.handleConsoleUpdating(drcDirection);
        Assert.assertEquals(0, monitorCase.getFailCount().get(drcDirection).intValue());
        Assert.assertEquals(1, delayMap.size(drcDirection));

        Mockito.when(healthService.isLocalDcUpdating(Mockito.anyString())).thenReturn(false);
        Mockito.when(healthService.isTargetDcUpdating(Mockito.anyString())).thenReturn(false);
        monitorCase.handleConsoleUpdating(drcDirection);
        Assert.assertEquals(1, monitorCase.getFailCount().get(drcDirection).intValue());
        Assert.assertEquals(1, delayMap.size(drcDirection));

        Mockito.when(healthService.isLocalDcUpdating(Mockito.anyString())).thenReturn(true);
        Mockito.when(healthService.isTargetDcUpdating(Mockito.anyString())).thenReturn(true);
        monitorCase.handleConsoleUpdating(drcDirection);
        Assert.assertEquals(0, monitorCase.getFailCount().get(drcDirection).intValue());
        Assert.assertEquals(1, delayMap.size(drcDirection));

        Mockito.when(healthService.isLocalDcUpdating(Mockito.anyString())).thenReturn(false);
        Mockito.when(healthService.isTargetDcUpdating(Mockito.anyString())).thenReturn(false);
        for(int i = 0; i < FAIL_THRESHOLD-1; ++i) {
            monitorCase.handleConsoleUpdating(drcDirection);
            Assert.assertEquals(i+1, monitorCase.getFailCount().get(drcDirection).intValue());
            Assert.assertEquals(1, delayMap.size(drcDirection));
        }

        monitorCase.handleConsoleUpdating(drcDirection);
        Assert.assertEquals(0, monitorCase.getFailCount().get(drcDirection).intValue());
        Assert.assertEquals(-1, delayMap.size(drcDirection));
    }

    @Test
    public void testIsAllConsoleUpdatingDb() {
        Mockito.when(healthService.isLocalDcUpdating(Mockito.anyString())).thenReturn(true);
        Mockito.when(healthService.isTargetDcUpdating(Mockito.anyString())).thenReturn(true);
        Assert.assertTrue(monitorCase.isAllConsoleUpdatingDb("A", "B"));

        Mockito.when(healthService.isLocalDcUpdating(Mockito.anyString())).thenReturn(true);
        Mockito.when(healthService.isTargetDcUpdating(Mockito.anyString())).thenReturn(false);
        Assert.assertFalse(monitorCase.isAllConsoleUpdatingDb("A", "B"));

        Mockito.when(healthService.isLocalDcUpdating(Mockito.anyString())).thenReturn(false);
        Mockito.when(healthService.isTargetDcUpdating(Mockito.anyString())).thenReturn(true);
        Assert.assertFalse(monitorCase.isAllConsoleUpdatingDb("A", "B"));

        Mockito.when(healthService.isLocalDcUpdating(Mockito.anyString())).thenReturn(false);
        Mockito.when(healthService.isTargetDcUpdating(Mockito.anyString())).thenReturn(false);
        Assert.assertFalse(monitorCase.isAllConsoleUpdatingDb("A", "B"));
    }

    @After
    public void tearDown() {
        try {
            if(null != writeSqlOperatorWrapper) {
                writeSqlOperatorWrapper.stop();
                writeSqlOperatorWrapper.dispose();
            }
        } catch (Exception e) {
            logger.error("tearDown: ", e);
        }

    }
}