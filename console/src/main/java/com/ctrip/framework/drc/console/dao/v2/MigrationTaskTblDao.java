package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MigrationTaskTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * @ClassName MigrationTaskTblDao
 * @Author haodongPan
 * @Date 2023/8/22 11:28
 * @Version: $
 */
@Repository
public class MigrationTaskTblDao extends AbstractDao<MigrationTaskTbl> {
    
    private static final String ID = "id";
    private static final String DELETED = "deleted";

    public MigrationTaskTblDao() throws SQLException {
        super(MigrationTaskTbl.class);
    }

    public Long insertWithReturnId(MigrationTaskTbl migrationTaskTbl) throws SQLException {
        KeyHolder keyHolder = new KeyHolder();
        insert(new DalHints(), keyHolder, migrationTaskTbl);
        return (Long) keyHolder.getKey();
    }
    
}
