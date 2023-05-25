package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationFilterMappingTbl;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/5/25 12:00
 */
@Repository
public class DbReplicationFilterMappingTblDao extends AbstractDao<DbReplicationFilterMappingTbl> {

    public DbReplicationFilterMappingTblDao() throws SQLException {
        super(DbReplicationFilterMappingTbl.class);
    }
}
