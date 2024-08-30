package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.core.monitor.reporter.CatEventMonitor;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.base.SQLResultSpec;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;
import java.sql.SQLException;

/**
 * Created by shiruixin
 * 2024/8/28 17:06
 */
public class OverseaDalTableDaoTest {
    DalHints dalHints = Mockito.mock(DalHints.class);
    SQLResultSpec sqlResultSpec = Mockito.mock(SQLResultSpec.class);
    SelectSqlBuilder sqlBuilder = Mockito.mock(SelectSqlBuilder.class);
    KeyHolder keyHolder = Mockito.mock(KeyHolder.class);

    OverseaDalTableDao dao;
    MockedStatic<DefaultEventMonitorHolder> mockedStatic;

    @Mock
    CatEventMonitor catEventMonitor;

    @Mock
    DefaultConsoleConfig consoleConfig;

    @InjectMocks
    ZookeeperTblDao dao1;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        dao = new OverseaDalTableDao(this.getClass());
        mockedStatic = Mockito.mockStatic(DefaultEventMonitorHolder.class);
        mockedStatic.when(DefaultEventMonitorHolder::getInstance).thenReturn(catEventMonitor);
    }

    @After
    public void tearDown() {
        mockedStatic.close();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testQuery() throws SQLException {
        dao.query("testSql", dalHints, sqlResultSpec, new Object());
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testQuery2() throws SQLException {
        dao.query("testSql", dalHints, new Object());
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testQuery3() throws SQLException {
        dao.query(sqlBuilder, dalHints);
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testQuery4() throws SQLException {
        dao.queryObject("testSql", dalHints, sqlResultSpec, new Object());
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testQuery5() throws SQLException {
        dao.queryFirst(sqlBuilder, dalHints);
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testQuery6() throws SQLException {
        dao.queryByPk(1, dalHints);
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testQuery7() throws SQLException {
        dao.queryByPk(new Object(), dalHints);
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testQuery8() throws SQLException {
        dao.queryBy(new Object(), dalHints);
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCount() throws SQLException {
        dao.count(sqlBuilder, dalHints);
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInsert() throws SQLException {
        dao.insert(dalHints, new Object());
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInsert2() throws SQLException {
        dao.insert(dalHints, Lists.newArrayList());
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInsert3() throws SQLException {
        dao.insert(dalHints, keyHolder, new Object());
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInsert4() throws SQLException {
        dao.insert(dalHints, keyHolder, Lists.newArrayList());
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInsert5() throws SQLException {
        dao.batchInsert(dalHints, Lists.newArrayList());
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInsert6() throws SQLException {
        dao.combinedInsert(dalHints, Lists.newArrayList());
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInsert7() throws SQLException {
        dao.combinedInsert(dalHints, keyHolder, Lists.newArrayList());
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }

}
