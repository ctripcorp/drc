package com.ctrip.framework.drc.console.dao;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import com.ctrip.framework.drc.console.dao.entity.ApplierUploadLogTbl;
import com.ctrip.framework.drc.console.dao.entity.ConflictLog;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.StatementParameters;
import com.ctrip.platform.dal.dao.sqlbuilder.FreeSelectSqlBuilder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;

/**
 * @author wjx王继欣
 * @date 2020-10-25
 */
public class ConflictLogDao extends AbstractDao<ConflictLog> {
    public ConflictLogDao() throws SQLException {
        super(ConflictLog.class);
    }

    public List<ConflictLog> queryLogByPage(int pageNo, int pageSize, String keyWord, DalHints hints)  throws SQLException {

        final SelectSqlBuilder builder = new SelectSqlBuilder();
        builder.selectAll().atPage(pageNo, pageSize).orderBy("id", false).like("cluster_name", "%"+keyWord+"%", Types.CHAR).from("conflict_log");
        return client.query(builder, new DalHints());
    }

    public int count(String keyWord)  throws SQLException {

        final SelectSqlBuilder builder = new SelectSqlBuilder();
        builder.selectCount().like("cluster_name", "%"+keyWord+"%", Types.CHAR).from("conflict_log");
        return client.count(builder, new DalHints()).intValue();
    }
}

