package com.ctrip.framework.drc.console.dao;

import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.DalTableDao;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.base.SQLResultSpec;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
/**
 * Created by shiruixin
 * 2024/8/28 10:02
 */
public class BaseDalTableDaoTest {
    @Mock
    DalTableDao<Object> dalTableDao;

    DalHints dalHints = Mockito.mock(DalHints.class);
    SQLResultSpec sqlResultSpec = Mockito.mock(SQLResultSpec.class);
    SelectSqlBuilder sqlBuilder = Mockito.mock(SelectSqlBuilder.class);
    KeyHolder keyHolder = Mockito.mock(KeyHolder.class);

    BaseDalTableDao baseDalDao;


    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        baseDalDao = new BaseDalTableDao();
        baseDalDao.setDalTableDao(dalTableDao);

    }
    @Test
    public void testQuery() throws SQLException {
        List<Object> expected = Collections.singletonList(1);
        Mockito.when(dalTableDao.query(anyString(), Mockito.any(DalHints.class), Mockito.any(SQLResultSpec.class), any())).thenReturn(expected);
        Assert.assertEquals(baseDalDao.query("testSql", dalHints, sqlResultSpec, new Object()), expected);

        Mockito.when(dalTableDao.query(anyString(), Mockito.any(DalHints.class), any())).thenReturn(expected);
        Assert.assertEquals(baseDalDao.query("testSql", dalHints, new Object()), expected);

        Mockito.when(dalTableDao.query(Mockito.any(SelectSqlBuilder.class), Mockito.any(DalHints.class))).thenReturn(expected);
        Assert.assertEquals(baseDalDao.query(sqlBuilder, dalHints), expected);

        when(dalTableDao.queryObject(anyString(), Mockito.any(DalHints.class), Mockito.any(SQLResultSpec.class), any())).thenReturn(expected);
        Assert.assertEquals(baseDalDao.queryObject("testSql", dalHints, sqlResultSpec, new Object()), expected);

        when(dalTableDao.queryFirst(Mockito.any(SelectSqlBuilder.class), Mockito.any(DalHints.class))).thenReturn(expected);
        Assert.assertEquals(baseDalDao.queryFirst(sqlBuilder, dalHints), expected);

        when(dalTableDao.queryByPk(Mockito.any(Number.class), Mockito.any(DalHints.class))).thenReturn(expected);
        Assert.assertEquals(baseDalDao.queryByPk(1, dalHints), expected);

        when(dalTableDao.queryByPk(Mockito.any(Object.class), Mockito.any(DalHints.class))).thenReturn(expected);
        Assert.assertEquals(baseDalDao.queryByPk(new Object(), dalHints), expected);

        Mockito.when(dalTableDao.queryBy(Mockito.any(Object.class), Mockito.any(DalHints.class))).thenReturn(expected);
        Assert.assertEquals(baseDalDao.queryBy(new Object(), dalHints), expected);
    }

    @Test
    public void testCount() throws SQLException {
        when(dalTableDao.count(Mockito.any(SelectSqlBuilder.class), Mockito.any(DalHints.class))).thenReturn(1);
        Assert.assertEquals(baseDalDao.count(sqlBuilder, dalHints), 1);
    }

    @Test
    public void testInsert() throws SQLException {
        int[] expected = new int[] {1};

        when(dalTableDao.insert(Mockito.any(DalHints.class), Mockito.any(Object.class))).thenReturn(1);
        Assert.assertEquals(baseDalDao.insert(dalHints, new Object()), 1);

        when(dalTableDao.insert(Mockito.any(DalHints.class), anyList())).thenReturn(expected);
        Assert.assertEquals(baseDalDao.insert(dalHints, Lists.newArrayList()), expected);

        when(dalTableDao.insert(Mockito.any(DalHints.class), Mockito.any(KeyHolder.class), Mockito.any(Object.class))).thenReturn(1);
        Assert.assertEquals(baseDalDao.insert(dalHints, keyHolder, new Object()), 1);

        when(dalTableDao.insert(Mockito.any(DalHints.class), Mockito.any(KeyHolder.class), anyList())).thenReturn(expected);
        Assert.assertEquals(baseDalDao.insert(dalHints, keyHolder, Lists.newArrayList()), expected);

        Mockito.when(dalTableDao.batchInsert(Mockito.any(DalHints.class), anyList())).thenReturn(expected);
        Assert.assertEquals(baseDalDao.batchInsert(dalHints, Lists.newArrayList()), expected);

        when(dalTableDao.combinedInsert(Mockito.any(DalHints.class), anyList())).thenReturn(1);
        Assert.assertEquals(baseDalDao.combinedInsert(dalHints, Lists.newArrayList()), 1);

        when(dalTableDao.combinedInsert(Mockito.any(DalHints.class), Mockito.any(KeyHolder.class), anyList())).thenReturn(1);
        Assert.assertEquals(baseDalDao.combinedInsert(dalHints, keyHolder, Lists.newArrayList()), 1);
    }

    @Test
    public void testDelete() throws SQLException {
        int[] expected = new int[] {1};

        when(dalTableDao.delete(Mockito.any(DalHints.class), Mockito.any(Object.class))).thenReturn(1);
        Assert.assertEquals(baseDalDao.delete(dalHints, new Object()), 1);

        when(dalTableDao.delete(Mockito.any(DalHints.class), anyList())).thenReturn(expected);
        Assert.assertEquals(baseDalDao.delete(dalHints, Lists.newArrayList()), expected);

        when(dalTableDao.delete(anyString(), Mockito.any(DalHints.class), any())).thenReturn(1);
        Assert.assertEquals(baseDalDao.delete("testSql", dalHints, new Object()), 1);

        Mockito.when(dalTableDao.batchDelete(Mockito.any(DalHints.class), anyList())).thenReturn(expected);
        Assert.assertEquals(baseDalDao.batchDelete(dalHints, Lists.newArrayList()), expected);
    }

    @Test
    public void testUpdata() throws SQLException {
        when(dalTableDao.update(Mockito.any(DalHints.class), Mockito.any(Object.class))).thenReturn(1);
        Assert.assertEquals(baseDalDao.update(dalHints, new Object()), 1);
    }


}
