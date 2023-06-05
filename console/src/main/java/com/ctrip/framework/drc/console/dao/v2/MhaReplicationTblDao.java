package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/5/25 12:03
 */
@Repository
public class MhaReplicationTblDao extends AbstractDao<MhaReplicationTbl> {

    public MhaReplicationTblDao() throws SQLException {
        super(MhaReplicationTbl.class);
    }
}
