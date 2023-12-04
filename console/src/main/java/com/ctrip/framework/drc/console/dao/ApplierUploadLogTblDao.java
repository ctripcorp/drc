package com.ctrip.framework.drc.console.dao;

import com.ctrip.platform.dal.dao.*;
import com.ctrip.platform.dal.dao.sqlbuilder.*;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import com.ctrip.framework.drc.console.dao.entity.ApplierUploadLogTbl;
import org.springframework.stereotype.Repository;


/**
 * @author wjx王继欣
 * @date 2020-01-20
 */
@Repository
public class ApplierUploadLogTblDao extends AbstractDao<ApplierUploadLogTbl> {

    public ApplierUploadLogTblDao() throws SQLException {
        super(ApplierUploadLogTbl.class);
    }

    public List<ApplierUploadLogTbl> queryLogByPage(int pageNo, int pageSize, DalHints hints)  throws SQLException {

        final SelectSqlBuilder builder = new SelectSqlBuilder();
        builder.selectAll().atPage(pageNo, pageSize).orderBy("datachange_lasttime", false).from("applier_upload_log_tbl");
        return client.query(builder, new DalHints());
    }

    /*
    搜索
     */

    public List<ApplierUploadLogTbl> queryLogByPage(int pageNo, int pageSize, String keyWord, DalHints hints)  throws SQLException {

        final SelectSqlBuilder builder = new SelectSqlBuilder();
        builder.selectAll().atPage(pageNo, pageSize).orderBy("datachange_lasttime", false).like("cluster_name", "%"+keyWord+"%", Types.CHAR).from("applier_upload_log_tbl");
        return client.query(builder, new DalHints());
    }

    public int count(String keyWord)  throws SQLException {

        final SelectSqlBuilder builder = new SelectSqlBuilder();
        builder.selectCount().like("cluster_name", "%"+keyWord+"%", Types.CHAR).from("applier_upload_log_tbl");
        return client.count(builder, new DalHints()).intValue();
    }
}
