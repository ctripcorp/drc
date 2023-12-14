package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.enums.ActionEnum;
import com.ctrip.framework.drc.console.mock.LocalMasterMySQLEndpointObservable;
import com.ctrip.framework.drc.console.mock.LocalSlaveMySQLEndpointObservable;
import com.ctrip.framework.drc.console.monitor.delay.config.DataCenterService;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.service.impl.openapi.OpenService;
import com.ctrip.framework.drc.console.service.v2.CentralService;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.response.UuidResponseVo;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
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
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @ClassName UuidMonitorTest
 * @Author haodongPan
 * @Date 2021/8/10 14:45
 * @Version: 1.0$
 */

public class UuidMonitorTest {

    @InjectMocks
    private UuidMonitor uuidMonitor;

    @Mock
    private DefaultCurrentMetaManager currentMetaManager;

    @Mock
    private DataCenterService dataCenterService;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Mock
    private Reporter reporter = DefaultReporterHolder.getInstance();
    
    @Mock
    private DefaultConsoleConfig consoleConfig;
    
    @Mock
    private CentralService centralService;


    private MySqlEndpoint mha1MasterEndpoint = new MySqlEndpoint("ip1", 3306, "root", "root", true);
    private MySqlEndpoint mha1SlaveEndpoint = new MySqlEndpoint("ip2", 3306, "root", "root", false);

    private MySqlEndpoint mha2MasterEndpoint = new MySqlEndpoint("ip3", 3306, "root", "root", true);
    private MySqlEndpoint mha2SlaveEndpoint = new MySqlEndpoint("ip4", 3306, "root", "root", false);
    private static final String UUID_ERROR_NUM_MEASUREMENT = "fx.drc.uuid.errorNums";
    
    
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        when(consoleConfig.getRegion()).thenReturn("sha");
        when(dataCenterService.getDc()).thenReturn("shaxy");
        when(consoleConfig.getPublicCloudRegion()).thenReturn(Sets.newHashSet("sha"));
        when(consoleConfig.getDcsInLocalRegion()).thenReturn(Sets.newHashSet("shaxy","shali"));
        Mockito.doNothing().when(currentMetaManager).addObserver(uuidMonitor);
        Mockito.doReturn(SWITCH_STATUS_ON).when(monitorTableSourceProvider).getUuidMonitorSwitch();
        Mockito.doReturn(SWITCH_STATUS_ON).when(monitorTableSourceProvider).getUuidCorrectSwitch();
        Mockito.doNothing().when(reporter).resetReportCounter(Mockito.any(), Mockito.anyLong(), eq(UUID_ERROR_NUM_MEASUREMENT));
        
    }

    @Test
    public void testMonitor() {
        try (MockedStatic<MySqlUtils> mysqlUtilsMock = mockStatic(MySqlUtils.class)) {
            when(centralService.getUuidInMetaDb(eq("mha1"),eq("ip1"),eq(3306))).thenReturn("uuid1");
            when(centralService.getUuidInMetaDb(eq("mha2"),eq("ip2"),eq(3306))).thenReturn("uuid2");
            when(centralService.getUuidInMetaDb(eq("mha1"),eq("ip3"),eq(3306))).thenReturn("uuid3");
            when(centralService.getUuidInMetaDb(eq("mha2"),eq("ip4"),eq(3306))).thenReturn("uuid4");
            mysqlUtilsMock.when(() -> MySqlUtils.getUuid(Mockito.eq(mha1MasterEndpoint),eq(true))).thenReturn("uuid1-version2");
            mysqlUtilsMock.when(() -> MySqlUtils.getUuid(Mockito.eq(mha1SlaveEndpoint),eq(false))).thenReturn("uuid2");
            mysqlUtilsMock.when(() -> MySqlUtils.getUuid(Mockito.eq(mha2MasterEndpoint),eq(true))).thenReturn("uuid3");
            mysqlUtilsMock.when(() -> MySqlUtils.getUuid(Mockito.eq(mha2SlaveEndpoint),eq(false))).thenReturn("uuid4-version2");
            when(centralService.correctMachineUuid(Mockito.any(MachineTbl.class))).thenReturn(1);

            uuidMonitor.initialize();
            uuidMonitor.isRegionLeader = true;
            MetaKey metaKey1 = new MetaKey("shali", "cluster1.mha1", "cluster1", "mha1");
            MetaKey metaKey2 = new MetaKey("shaxy","cluster2.mha2","cluster2","mha2");
            // rds master & slave ,mha master & slave
            uuidMonitor.update(new Triple<>(metaKey1, mha1MasterEndpoint, ActionEnum.ADD), new LocalMasterMySQLEndpointObservable());
            uuidMonitor.update(new Triple<>(metaKey2, mha2MasterEndpoint, ActionEnum.ADD), new LocalMasterMySQLEndpointObservable());
            uuidMonitor.update(new Triple<>(metaKey1, mha1SlaveEndpoint, ActionEnum.ADD), new LocalSlaveMySQLEndpointObservable());
            uuidMonitor.update(new Triple<>(metaKey2, mha2SlaveEndpoint, ActionEnum.ADD), new LocalSlaveMySQLEndpointObservable());
            uuidMonitor.scheduledTask();
            
            verify(centralService,times(2)).correctMachineUuid(Mockito.any(MachineTbl.class));
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
    }
    

}
