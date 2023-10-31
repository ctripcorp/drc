package com.ctrip.framework.drc.console.dao.log;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictAutoHandleBatchTbl;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/10/30 20:18
 */
@Repository
public class ConflictAutoHandleBatchTblDao extends AbstractDao<ConflictAutoHandleBatchTbl> {

    public ConflictAutoHandleBatchTblDao() throws SQLException {
        super(ConflictAutoHandleBatchTbl.class);
    }
}
