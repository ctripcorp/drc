package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.enums.ActionEnum;
import com.ctrip.framework.drc.console.mock.LocalMasterMySQLEndpointObservable;
import com.ctrip.framework.drc.console.mock.LocalSlaveMySQLEndpointObservable;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.service.impl.openapi.OpenService;
import com.ctrip.framework.drc.console.vo.response.AbstractResponse;
import com.ctrip.framework.drc.console.vo.response.UuidResponseVo;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.unidal.tuple.Triple;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.ctrip.framework.drc.console.monitor.MockTest.times;
import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
import static com.ctrip.framework.drc.console.utils.UTConstants.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

/**
 * @ClassName UuidMonitorTest
 * @Author haodongPan
 * @Date 2021/8/10 14:45
 * @Version: 1.0$
 */

public class UuidMonitorTest extends AbstractTest {

    @InjectMocks
    private UuidMonitor uuidMonitor;

    @Mock
    private DefaultCurrentMetaManager currentMetaManager;

    @Mock
    private DbClusterSourceProvider sourceProvider;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Mock
    private Reporter reporter = DefaultReporterHolder.getInstance();

    @Mock
    private MachineTblDao machineTblDao;

    @Mock
    private DefaultConsoleConfig consoleConfig;
    
    @Mock
    private OpenService openService;


    private static final String UUID_COMMAND = "show  global  variables  like \"server_uuid\";";
    private MySqlEndpoint mha3MasterEndpoint = new MySqlEndpoint(REMOTE_MYSQL_IP, REMOTE_MYSQL_Port, REMOTE_MYSQL_User, REMOTE_MYSQL_PassWord, true);
    private MySqlEndpoint mha3SlaveEndpoint = new MySqlEndpoint(REMOTE_MYSQL_IP, REMOTE_MYSQL_Port2, REMOTE_MYSQL_User, REMOTE_MYSQL_PassWord, false);
    private MySqlEndpoint mha1MasterEndpoint = new MySqlEndpoint(CI_MYSQL_IP, CI_PORT1, CI_MYSQL_USER, CI_MYSQL_PASSWORD, true);
    private MySqlEndpoint mha1SlaveEndpoint = new MySqlEndpoint(CI_MYSQL_IP, CI_PORT2, CI_MYSQL_USER, CI_MYSQL_PASSWORD, false);
    private MySqlEndpoint mha2MasterEndpoint = new MySqlEndpoint(MYSQL_IP, SRC_PORT, MYSQL_USER, MYSQL_PASSWORD, true);
    private MySqlEndpoint mha2SlaveEndpoint = new MySqlEndpoint(MYSQL_IP, SRC_PORT, MYSQL_USER, MYSQL_PASSWORD, false);
    private static final String REMOTE_MYSQL_IP = "10.2.83.109"; // any uuid can
    private static final int REMOTE_MYSQL_Port = 3307;
    private static final int REMOTE_MYSQL_Port2 = 3306;
    private static final String REMOTE_MYSQL_User = "root";
    private static final String REMOTE_MYSQL_PassWord = "root";
    private static final String UUID_ERROR_NUM_MEASUREMENT = "fx.drc.uuid.errorNums";


    private static String remote_server_uuid = getRemoteUUID();
    private static WriteSqlOperatorWrapper writeSqlOperatorWrapper;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        
        Set<String> publicCloudDc  = new HashSet<>();
        publicCloudDc.add("shali");
        Mockito.doReturn(publicCloudDc).when(consoleConfig).getPublicCloudDc();
        Map<String, String> consoleDcInfos = Maps.newHashMap();
        consoleDcInfos.put("shaoy", "uri");
        Mockito.when(consoleConfig.getConsoleDcInfos()).thenReturn(consoleDcInfos);
        Mockito.doReturn(DC1).when(sourceProvider).getLocalDcName();
//        Mockito.doNothing().when(currentMetaManager).addObserver(uuidMonitor);
        Mockito.doReturn(SWITCH_STATUS_ON).when(monitorTableSourceProvider).getUuidMonitorSwitch();
        Mockito.doReturn(SWITCH_STATUS_ON).when(monitorTableSourceProvider).getUuidCorrectSwitch();
        
        
        // error uuid hickwall do not report

        Mockito.doNothing().when(reporter).resetReportCounter(Mockito.any(), Mockito.anyLong(), eq(UUID_ERROR_NUM_MEASUREMENT));
        MachineTbl sample = new MachineTbl();
        sample.setUuid(remote_server_uuid);
        Mockito.doReturn(sample).when(machineTblDao).queryByIpPort(eq(REMOTE_MYSQL_IP), eq(REMOTE_MYSQL_Port));
        Mockito.doReturn(null).when(machineTblDao).queryByIpPort(eq(REMOTE_MYSQL_IP), eq(REMOTE_MYSQL_Port2));
        uuidMonitor.initialize();
    }

    private static String getRemoteUUID() {
        ReadResource readResource = null;
        try {
            if (writeSqlOperatorWrapper == null) {
                writeSqlOperatorWrapper = new WriteSqlOperatorWrapper(
                        new DefaultEndPoint(REMOTE_MYSQL_IP, REMOTE_MYSQL_Port, REMOTE_MYSQL_User, REMOTE_MYSQL_PassWord));
                writeSqlOperatorWrapper.initialize();
                writeSqlOperatorWrapper.start();
            }
            GeneralSingleExecution execution = new GeneralSingleExecution(UUID_COMMAND);
            readResource = writeSqlOperatorWrapper.select(execution);
            ResultSet rs = readResource.getResultSet();
            if (rs == null) {
                return null;
            }
            rs.next();
            String res = rs.getString(2);
            return res;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (readResource != null) {
                readResource.close();
            }
        }
        return null;
    }

    @Test
    public void testInit() {
        verify(sourceProvider, times(1)).getLocalDcName();
        verify(currentMetaManager, times(1)).addObserver(uuidMonitor);
    }

    @Test
    public void testMonitorUuid() {
        uuidMonitor.masterMySQLEndpointMap.put(META_KEY1, mha3MasterEndpoint);
        uuidMonitor.slaveMySQLEndpointMap.put(META_KEY1, mha3SlaveEndpoint);

        uuidMonitor.scheduledTask();
        Assert.assertEquals(SWITCH_STATUS_ON, monitorTableSourceProvider.getUuidMonitorSwitch());
    }

    @Test
    public void testUUidError() throws SQLException {

        uuidMonitor.masterMySQLEndpointMap.put(META_KEY1, mha3MasterEndpoint);
        uuidMonitor.slaveMySQLEndpointMap.put(META_KEY1, mha3SlaveEndpoint);
        MachineTbl sample = new MachineTbl();
        sample.setUuid("errorUUID");
        Mockito.doReturn(sample).when(machineTblDao).queryByIpPort(eq(REMOTE_MYSQL_IP), eq(REMOTE_MYSQL_Port));

        uuidMonitor.scheduledTask();
    }

    @Test
    public void testObserve() {
        Assert.assertEquals(0, uuidMonitor.masterMySQLEndpointMap.size());
        Assert.assertEquals(0, uuidMonitor.slaveMySQLEndpointMap.size());

        uuidMonitor.update(new Triple<>(META_KEY3, mha1MasterEndpoint, ActionEnum.ADD), new LocalMasterMySQLEndpointObservable());
        uuidMonitor.update(new Triple<>(META_KEY3, mha1SlaveEndpoint, ActionEnum.ADD), new LocalSlaveMySQLEndpointObservable());
        Assert.assertEquals(0, uuidMonitor.masterMySQLEndpointMap.size());
        Assert.assertEquals(0, uuidMonitor.slaveMySQLEndpointMap.size());

        uuidMonitor.update(new Triple<>(META_KEY1, mha1MasterEndpoint, ActionEnum.ADD), new LocalMasterMySQLEndpointObservable());
        uuidMonitor.update(new Triple<>(META_KEY1, mha1SlaveEndpoint, ActionEnum.ADD), new LocalSlaveMySQLEndpointObservable());
        Assert.assertEquals(1, uuidMonitor.masterMySQLEndpointMap.size());
        Assert.assertEquals(1, uuidMonitor.slaveMySQLEndpointMap.size());
        Assert.assertEquals(mha1MasterEndpoint, uuidMonitor.masterMySQLEndpointMap.get(META_KEY1));
        Assert.assertEquals(mha1SlaveEndpoint, uuidMonitor.slaveMySQLEndpointMap.get(META_KEY1));

        uuidMonitor.update(new Triple<>(META_KEY1, mha2MasterEndpoint, ActionEnum.UPDATE), new LocalMasterMySQLEndpointObservable());
        uuidMonitor.update(new Triple<>(META_KEY1, mha2SlaveEndpoint, ActionEnum.UPDATE), new LocalSlaveMySQLEndpointObservable());
        Assert.assertEquals(1, uuidMonitor.masterMySQLEndpointMap.size());
        Assert.assertEquals(1, uuidMonitor.slaveMySQLEndpointMap.size());
        Assert.assertEquals(mha2MasterEndpoint, uuidMonitor.masterMySQLEndpointMap.get(META_KEY1));
        Assert.assertEquals(mha2SlaveEndpoint, uuidMonitor.slaveMySQLEndpointMap.get(META_KEY1));

        uuidMonitor.update(new Triple<>(META_KEY1, mha2MasterEndpoint, ActionEnum.DELETE), new LocalMasterMySQLEndpointObservable());
        uuidMonitor.update(new Triple<>(META_KEY1, mha2SlaveEndpoint, ActionEnum.DELETE), new LocalSlaveMySQLEndpointObservable());
        Assert.assertEquals(0, uuidMonitor.masterMySQLEndpointMap.size());
        Assert.assertEquals(0, uuidMonitor.slaveMySQLEndpointMap.size());
    }
    
    
    @Test
    public void testRemoteDc() {
        uuidMonitor.masterMySQLEndpointMap.put(META_KEY1, mha3MasterEndpoint);
        
        // DC1 as publicCloudDC
        Set<String> publicCloudDc  = new HashSet<>();
        publicCloudDc.add(DC1);
        Mockito.doReturn(publicCloudDc).when(consoleConfig).getPublicCloudDc();
        
        UuidResponseVo responseVoWithErrorUuid = new UuidResponseVo();
        responseVoWithErrorUuid.setStatus(0);
        MachineTbl machineTbl = new MachineTbl();
        machineTbl.setUuid("errorUUID");
        responseVoWithErrorUuid.setData(machineTbl);
        Mockito.doReturn(responseVoWithErrorUuid).when(openService).getUUIDFromRemoteDC(Mockito.anyString(),Mockito.anyMap());

        AbstractResponse<String> updateResponse = new AbstractResponse<>();
        updateResponse.setStatus(0);
        Mockito.doReturn(updateResponse).when(openService).updateUuidByMachineTbl(Mockito.anyString(),Mockito.any(MachineTbl.class));
        
        uuidMonitor.scheduledTask();
    }


}
