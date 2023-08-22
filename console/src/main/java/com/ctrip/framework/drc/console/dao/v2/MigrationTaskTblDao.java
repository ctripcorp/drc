package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MigrationTaskTbl;
import java.sql.SQLException;
import org.springframework.stereotype.Repository;

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
    
    
}
