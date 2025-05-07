package com.ctrip.framework.drc.console.dao;

import com.ctrip.platform.dal.dao.*;
import com.ctrip.platform.dal.dao.base.SQLResultSpec;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import com.ctrip.platform.dal.dao.task.DalRequestExecutor;
import com.ctrip.platform.dal.dao.task.DalTaskFactory;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by shiruixin
 * 2024/8/26 16:54
 */
public class BaseDalTableDao<T>{
    private DalTableDao<T> dalTableDao;

    public BaseDalTableDao(){

    }

    public BaseDalTableDao(DalParser<T> parser) {
        this(parser, DalClientFactory.getTaskFactory());
    }

    public BaseDalTableDao(DalParser<T> parser, DalTaskFactory factory) {
        this(parser, factory, new DalRequestExecutor());
    }

    public BaseDalTableDao(DalParser<T> parser, DalTaskFactory factory, DalRequestExecutor executor) {
        dalTableDao = new DalTableDao<>(parser, factory, executor);
    }

    public void setDalTableDao(DalTableDao<T> dalTableDao) {
        this.dalTableDao = dalTableDao;
    }

    public <K> List<K> query(String sql, DalHints dalHints, SQLResultSpec result, Object... args) throws SQLException {
        return dalTableDao.query(sql, dalHints, result, args);
    }

    public List<T> query(String sql, DalHints dalHints, Object... args) throws SQLException {
        return dalTableDao.query(sql, dalHints, args);
    }

    public <K> K queryObject(String sql, DalHints dalHints, SQLResultSpec result, Object... args) throws SQLException {
        return dalTableDao.queryObject(sql, dalHints, result, args);
    }

    public T queryFirst(SelectSqlBuilder selectBuilder, DalHints hints) throws SQLException {
        return dalTableDao.queryFirst(selectBuilder, hints);
    }

    public List<T> query(SelectSqlBuilder selectBuilder, DalHints hints) throws SQLException {
        return dalTableDao.query(selectBuilder, hints);
    }

    public T queryByPk(Number id, DalHints hints) throws SQLException {
        return dalTableDao.queryByPk(id, hints);
    }

    public T queryByPk(T pk, DalHints hints) throws SQLException {
        return dalTableDao.queryByPk(pk, hints);
    }

    public List<T> queryBy(T sample, DalHints hints) throws SQLException {
        return dalTableDao.queryBy(sample, hints);
    }

    public Number count(SelectSqlBuilder selectBuilder, DalHints hints) throws SQLException {
        return dalTableDao.count(selectBuilder, hints);
    }

    public int insert(DalHints hints, T daoPojo) throws SQLException {
        return dalTableDao.insert(hints, daoPojo);
    }

    public int[] insert(DalHints hints, List<T> daoPojos) throws SQLException {
        return dalTableDao.insert(hints, daoPojos);
    }

    public int insert(DalHints hints, KeyHolder keyHolder, T daoPojo) throws SQLException {
        return dalTableDao.insert(hints, keyHolder, daoPojo);
    }

    public int[] insert(DalHints hints, KeyHolder keyHolder, List<T> daoPojos) throws SQLException {
        return dalTableDao.insert(hints, keyHolder, daoPojos);
    }

    public int[] batchInsert(DalHints hints, List<T> daoPojos) throws SQLException {
        return dalTableDao.batchInsert(hints, daoPojos);
    }

    public int combinedInsert(DalHints hints, List<T> daoPojos) throws SQLException {
        return dalTableDao.combinedInsert(hints, daoPojos);
    }

    public int combinedInsert(DalHints hints, KeyHolder keyHolder, List<T> daoPojos) throws SQLException {
        return dalTableDao.combinedInsert(hints, keyHolder, daoPojos);
    }

    public int delete(DalHints hints, T daoPojo) throws SQLException {
        return dalTableDao.delete(hints, daoPojo);
    }

    public int[] delete(DalHints hints, List<T> daoPojos) throws SQLException {
        return dalTableDao.delete(hints, daoPojos);
    }

    public int delete(String sql, DalHints dalHints, Object... args) throws SQLException {
        return dalTableDao.delete(sql, dalHints, args);
    }

    public int[] batchDelete(DalHints hints, List<T> daoPojos) throws SQLException {
        return dalTableDao.batchDelete(hints, daoPojos);
    }

    public int update(DalHints hints, T daoPojo) throws SQLException {
        return dalTableDao.update(hints, daoPojo);
    }

    public int[] update(DalHints hints, List<T> daoPojos) throws SQLException {
        return dalTableDao.update(hints, daoPojos);
    }

    public int[] batchUpdate(DalHints hints, List<T> daoPojos) throws SQLException {
        return dalTableDao.batchUpdate(hints, daoPojos);
    }
}
