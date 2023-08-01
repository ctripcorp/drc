package com.ctrip.framework.drc.console.service.v2.impl;

import static org.junit.Assert.*;

import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DbMetaCorrectServiceImplTest {

    @InjectMocks private DbMetaCorrectServiceImpl dbMetaCorrectService;
    
    @Mock private MhaTblV2Dao mhaTblV2Dao;

    @Mock private ReplicatorGroupTblDao rGroupTblDao;

    @Mock private ReplicatorTblDao replicatorTblDao;

    @Mock private ResourceTblDao resourceTblDao;

    @Mock private MachineTblDao machineTblDao;

    @Mock private MonitorTableSourceProvider monitorTableSourceProvider;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testUpdateMasterReplicatorIfChange() {
    }

    @Test
    public void testMhaInstancesChange() {
    }

    @Test
    public void testMhaMasterDbChange() {
    }

    @Test
    public void testCheckChange() {
    }
}