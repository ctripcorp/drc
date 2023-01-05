package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ColumnsFilterTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.springframework.stereotype.Repository;
/**
 * @ClassName ColumnsFilterTblDao
 * @Author haodongPan
 * @Date 2023/1/4 15:54
 * @Version: $
 */

@Repository
public class ColumnsFilterTblDao extends AbstractDao<ColumnsFilterTbl>{

    public ColumnsFilterTblDao() throws SQLException {
        super(ColumnsFilterTbl.class);
    }

    public List<ColumnsFilterTbl> queryByDataMediaId(Long dataMediaId, Integer deleted) throws SQLException {
        if (dataMediaId == null) {
            throw new IllegalArgumentException("build sql: query ColumnsFilterTbl By dataMediaId ,but it is empty");
        }
        SelectSqlBuilder builder = new SelectSqlBuilder();
        builder.selectAll().equal("data_media_id", dataMediaId, Types.BIGINT)
                .and().equal("deleted", deleted, Types.TINYINT);
        return client.query(builder,new DalHints());
    }
}
