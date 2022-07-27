package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.dao.ApplierGroupTblDao;
import com.ctrip.framework.drc.console.dao.MhaTblDao;
import com.ctrip.framework.drc.console.dao.entity.ApplierGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.List;


public class ApplyModeMigrateTaskTest {
    
    @InjectMocks
    ApplyModeMigrateTask applyModeMigrateTask;

    @Mock
    private DalUtils dalUtils = DalUtils.getInstance();
    
    @Mock
    private MhaTblDao mhaTblDao = dalUtils.getMhaTblDao();
    
    @Mock
    private ApplierGroupTblDao applierGroupTblDao = dalUtils.getApplierGroupTblDao();
    
    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Before
    public void setUp() throws Exception{
        MockitoAnnotations.openMocks(this);
        // mock data
        MhaTbl mhaTbl = new MhaTbl();
        mhaTbl.setId(1L);
        mhaTbl.setMhaName("mha1");
        List<MhaTbl> mhaTbls = Lists.newArrayList(mhaTbl);
        
        
        Mockito.when(monitorTableSourceProvider.getApplyModeMigrateSwitch()).thenReturn("on");
        Mockito.when(mhaTblDao.queryByDeleted(Mockito.anyInt())).thenReturn(mhaTbls);
        Mockito.when(mhaTblDao.update(Mockito.any(MhaTbl.class))).thenReturn(1);
    }
    
    
    @Test
    public void testScheduledTask() throws Exception {
        ApplierGroupTbl applierGroup1 = new ApplierGroupTbl();
        applierGroup1.setApplyMode(1);
        ApplierGroupTbl applierGroup2 = new ApplierGroupTbl();
        applierGroup2.setApplyMode(1);
        List<ApplierGroupTbl> applierGroupTbls = Lists.newArrayList(applierGroup1, applierGroup2);

        Mockito.when(applierGroupTblDao.queryById(Mockito.eq(1L),Mockito.anyInt())).thenReturn(applierGroupTbls);
        applyModeMigrateTask.scheduledTask();
        
        applierGroup2.setApplyMode(0);
        applyModeMigrateTask.scheduledTask();
        
        Mockito.when(applierGroupTblDao.queryById(Mockito.eq(1L),Mockito.anyInt())).thenThrow(new SQLException("sql error"));
        applyModeMigrateTask.scheduledTask();
    }
    
}