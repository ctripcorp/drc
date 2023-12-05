package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.dao.BuTblDao;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.RouteTblDao;
import com.ctrip.framework.drc.console.dao.entity.RouteTbl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;

import static com.ctrip.framework.drc.console.service.v2.MetaGeneratorBuilder.*;

public class DrcMaintenanceServiceImplTest extends AbstractTest {

    @InjectMocks
    private DrcMaintenanceServiceImpl drcMaintenanceService;
    @Mock
    private BuTblDao buTblDao;
    @Mock
    private DcTblDao dcTblDao;
    @Mock
    private RouteTblDao routeTblDao;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testDeleteRoute() throws SQLException {
        Mockito.when(buTblDao.queryByBuName(Mockito.anyString())).thenReturn(getButbls().get(0));
        Mockito.when(dcTblDao.queryByDcName(Mockito.anyString())).thenReturn(getDcTbls().get(0));
        Mockito.when(routeTblDao.queryAllExist()).thenReturn(getRouteTbls());
        Mockito.when(routeTblDao.update(Mockito.any(RouteTbl.class))).thenReturn(1);

        drcMaintenanceService.deleteRoute("route", "srcDc", "dstDc", "console");
        Mockito.verify(routeTblDao, Mockito.times(1)).update(Mockito.any(RouteTbl.class));
    }
}
