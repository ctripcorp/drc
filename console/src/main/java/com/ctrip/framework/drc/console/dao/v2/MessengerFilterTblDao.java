package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MessengerFilterTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by dengquanliang
 * 2023/5/31 17:26
 */
@Repository
public class MessengerFilterTblDao extends AbstractDao<MessengerFilterTbl> {

    private static final String ID = "id";
    private static final String DELETED = "deleted";

    public MessengerFilterTblDao() throws SQLException {
        super(MessengerFilterTbl.class);
    }

    public MessengerFilterTbl queryById(long id) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(ID, id, Types.BIGINT).and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return client.queryFirst(sqlBuilder, new DalHints());
    }
}
