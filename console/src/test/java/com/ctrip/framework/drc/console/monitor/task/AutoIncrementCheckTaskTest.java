package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.vo.check.v2.AutoIncrementVo;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/12/18 14:54
 */
public class AutoIncrementCheckTaskTest {

    @InjectMocks
    private AutoIncrementCheckTask task;
    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private MysqlServiceV2 mysqlServiceV2;
    @Mock
    private DcTblDao dcTblDao;
    @Mock
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
    @Mock
    private Reporter reporter;

    @Before
    public void setUp(){
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testScheduledTask() throws SQLException {
        Mockito.doNothing().when(reporter).reportResetCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.anyString());
        Mockito.when(mhaReplicationTblDao.queryAllExist()).thenReturn(getMhaReplicationTbls());
        Mockito.when(mhaTblV2Dao.queryAllExist()).thenReturn(getMhaTbls());
        Mockito.when(dcTblDao.queryAllExist()).thenReturn(getDcTbls());

        Mockito.when(mysqlServiceV2.getAutoIncrementAndOffset(Mockito.anyString())).thenReturn(new AutoIncrementVo(2, 2));
        Mockito.when(mysqlServiceV2.getAutoIncrementAndOffset(Mockito.eq("mhaName1"))).thenReturn(new AutoIncrementVo(2, 1));

        task.checkAutoIncrement();
        Mockito.verify(reporter, Mockito.times(2)).reportResetCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.anyString());
    }

    private List<MhaReplicationTbl> getMhaReplicationTbls() {
        MhaReplicationTbl tbl1 = new MhaReplicationTbl();
        tbl1.setSrcMhaId(1L);
        tbl1.setDstMhaId(2L);
        tbl1.setDrcStatus(1);

        MhaReplicationTbl tbl2 = new MhaReplicationTbl();
        tbl2.setSrcMhaId(2L);
        tbl2.setDstMhaId(1L);
        tbl2.setDrcStatus(1);

        MhaReplicationTbl tbl3 = new MhaReplicationTbl();
        tbl3.setSrcMhaId(1L);
        tbl3.setDstMhaId(3L);
        tbl3.setDrcStatus(1);

        MhaReplicationTbl tbl4 = new MhaReplicationTbl();
        tbl4.setSrcMhaId(3L);
        tbl4.setDstMhaId(1L);
        tbl4.setDrcStatus(1);


        MhaReplicationTbl tbl5 = new MhaReplicationTbl();
        tbl5.setSrcMhaId(4L);
        tbl5.setDstMhaId(1L);
        tbl5.setDrcStatus(1);
        return Lists.newArrayList(tbl1, tbl2, tbl3, tbl4, tbl5);
    }

    private List<DcTbl> getDcTbls() {
        DcTbl dcTbl0 = new DcTbl();
        dcTbl0.setId(1L);
        dcTbl0.setRegionName("sha");

        DcTbl dcTbl1 = new DcTbl();
        dcTbl1.setId(2L);
        dcTbl1.setRegionName("sin");

        DcTbl dcTbl2 = new DcTbl();
        dcTbl2.setId(3L);
        dcTbl2.setRegionName("fra");

        return Lists.newArrayList(dcTbl0, dcTbl1, dcTbl2);
    }


    private List<MhaTblV2> getMhaTbls() {
        MhaTblV2 mha1 = new MhaTblV2();
        mha1.setDcId(1L);
        mha1.setId(1L);
        mha1.setMhaName("mhaName1");

        MhaTblV2 mha2 = new MhaTblV2();
        mha2.setDcId(2L);
        mha2.setId(2L);
        mha2.setMhaName("mhaName2");

        MhaTblV2 mha3 = new MhaTblV2();
        mha3.setDcId(3L);
        mha3.setId(3L);
        mha3.setMhaName("mhaName3");

        MhaTblV2 mha4 = new MhaTblV2();
        mha4.setDcId(3L);
        mha4.setId(4L);
        mha4.setMhaName("mhaName4");

        return Lists.newArrayList(mha1, mha2, mha3, mha4);
    }

}
