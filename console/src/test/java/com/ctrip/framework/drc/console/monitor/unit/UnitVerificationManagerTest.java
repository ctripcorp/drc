package com.ctrip.framework.drc.console.monitor.unit;

import com.ctrip.framework.drc.console.dao.entity.MhaGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaGenerator;
import com.ctrip.framework.drc.console.service.monitor.impl.ConsistencyConsistencyMonitorServiceImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;

public class UnitVerificationManagerTest {

    @InjectMocks
    private UnitVerificationManager unitVerificationManager;

    @Mock
    private MetaGenerator metaService;

    @Mock
    private MetaInfoServiceImpl metaInfoService;

    @Mock
    private ConsistencyConsistencyMonitorServiceImpl monitorService;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Mockito.doReturn(SWITCH_STATUS_ON).when(monitorTableSourceProvider).getUnitVericationManagerSwitch();
    }

    @Test
    public void testInitlialize() {
        UnitVerificationManager manager = new UnitVerificationManager();
        manager.initialize();
        manager.destroy();
    }

    @Test
    public void testAddAndDelete() throws SQLException {
        // init verifying group 1, 2
        unitVerificationManager.verifyingGroupIds.add(1L);
        unitVerificationManager.verifyingGroupIds.add(2L);

        // try to add 3 and delete 2
        MhaGroupTbl mhaGroupTbl1 = new MhaGroupTbl();
        mhaGroupTbl1.setId(1L);
        mhaGroupTbl1.setUnitVerificationSwitch(1);
        MhaGroupTbl mhaGroupTbl2 = new MhaGroupTbl();
        mhaGroupTbl2.setId(2L);
        mhaGroupTbl2.setUnitVerificationSwitch(0);
        MhaGroupTbl mhaGroupTbl3 = new MhaGroupTbl();
        mhaGroupTbl3.setId(3L);
        mhaGroupTbl3.setUnitVerificationSwitch(1);
        List<MhaGroupTbl> mhaGroupTbls = Arrays.asList(mhaGroupTbl1, mhaGroupTbl2, mhaGroupTbl3);
        Mockito.doReturn(mhaGroupTbls).when(metaService).getMhaGroupTbls();

        Mockito.doReturn(true).when(monitorService).addUnitVerification(Mockito.anyLong());
        Mockito.doReturn(true).when(monitorService).deleteUnitVerification(Mockito.anyLong());

        Mockito.doThrow(new SQLException()).when(metaInfoService).getMhaTbls(Mockito.anyLong());
        unitVerificationManager.scheduledTask();
        Assert.assertTrue(weakEquals(Arrays.asList(1L, 2L), unitVerificationManager.verifyingGroupIds));

        MhaTbl mhaTbl1 = new MhaTbl(); mhaTbl1.setMhaName("mha1oy");
        MhaTbl mhaTbl2 = new MhaTbl(); mhaTbl2.setMhaName("mha1rb");
        MhaTbl mhaTbl3 = new MhaTbl(); mhaTbl3.setMhaName("mha2oy");
        MhaTbl mhaTbl4 = new MhaTbl(); mhaTbl4.setMhaName("mha2rb");
        MhaTbl mhaTbl5 = new MhaTbl(); mhaTbl5.setMhaName("mha3oy");
        MhaTbl mhaTbl6 = new MhaTbl(); mhaTbl6.setMhaName("mha3rb");
        Mockito.doReturn(Arrays.asList(mhaTbl1, mhaTbl2)).when(metaInfoService).getMhaTbls(1L);
        Mockito.doReturn(Arrays.asList(mhaTbl3, mhaTbl4)).when(metaInfoService).getMhaTbls(2L);
        Mockito.doReturn(Arrays.asList(mhaTbl5, mhaTbl6)).when(metaInfoService).getMhaTbls(3L);
        unitVerificationManager.scheduledTask();
        Assert.assertTrue(weakEquals(Arrays.asList(1L, 3L), unitVerificationManager.verifyingGroupIds));
    }

    public static <T> boolean weakEquals(List<T> arr1, List<T> arr2) {
        if(arr1.size() != arr2.size()) return false;
        Set<T> set1 = new HashSet<>(arr1);
        Set<T> set2 = new HashSet<>(arr2);
        if(set1.size() != set2.size()) return false;
        for(T t : set1) {
            if(!set2.contains(t)) return false;
        }
        return true;
    }
}
