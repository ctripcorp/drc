package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.platform.dal.dao.*;
import com.ctrip.platform.dal.dao.base.SQLResultSpec;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;

import java.sql.SQLException;
import java.util.List;

/**
  * Created by shiruixin
  * 2024/8/26 19:40
  */
public class OverseaDalTableDao<T> extends BaseDalTableDao<T>{
    private Class<T> clazz;

    public OverseaDalTableDao(Class<T> clazz) {
        this.clazz = clazz;
    }

    public <K> List<K> query(String sql, DalHints dalHints, SQLResultSpec result, Object... args) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support query");
    }

    public List<T> query(String sql, DalHints dalHints, Object... args) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support query");
    }

    public <K> K queryObject(String sql, DalHints dalHints, SQLResultSpec result, Object... args) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support queryObject");
    }

    public T queryFirst(SelectSqlBuilder selectBuilder, DalHints hints) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support queryFirst");
    }

    public List<T> query(SelectSqlBuilder selectBuilder, DalHints hints) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support query");
    }

    public T queryByPk(Number id, DalHints hints) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support queryByPk");
    }

    public T queryByPk(T pk, DalHints hints) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support queryByPk");
    }

    public List<T> queryBy(T sample, DalHints hints) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support queryBy");
    }

    public Number count(SelectSqlBuilder selectBuilder, DalHints hints) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support count");
    }

    public int insert(DalHints hints, T daoPojo) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support insert");
    }

    public int[] insert(DalHints hints, List<T> daoPojos) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support insert");
    }

    public int insert(DalHints hints, KeyHolder keyHolder, T daoPojo) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support insert");
    }

    public int[] insert(DalHints hints, KeyHolder keyHolder, List<T> daoPojos) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support insert");
    }

    public int[] batchInsert(DalHints hints, List<T> daoPojos) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support batchInsert");
    }

    public int combinedInsert(DalHints hints, List<T> daoPojos) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support combinedInsert");
    }

    public int combinedInsert(DalHints hints, KeyHolder keyHolder, List<T> daoPojos) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support combinedInsert");
    }

    public int delete(DalHints hints, T daoPojo) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support delete");
    }

    public int[] delete(DalHints hints, List<T> daoPojos) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support delete");
    }

    public int delete(String sql, DalHints dalHints, Object... args) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support delete");
    }

    public int[] batchDelete(DalHints hints, List<T> daoPojos) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support batchDelete");
    }

    public int update(DalHints hints, T daoPojo) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support update");
    }

    public int[] update(DalHints hints, List<T> daoPojos) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support update");
    }

    public int[] batchUpdate(DalHints hints, List<T> daoPojos) throws SQLException {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.oversea.dao.query", clazz.getName());
        throw new UnsupportedOperationException("Oversea DalTableDao doesn't support batchUpdate");
    }
}
