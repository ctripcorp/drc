package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/5/25 12:01
 */
@Repository
public class DbReplicationTblDao extends AbstractDao<DbReplicationTbl> {

    public DbReplicationTblDao() throws SQLException {
        super(DbReplicationTbl.class);
    }
}
