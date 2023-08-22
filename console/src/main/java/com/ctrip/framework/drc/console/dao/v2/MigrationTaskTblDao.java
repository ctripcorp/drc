package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MigrationTaskTbl;
import com.ctrip.framework.drc.console.param.v2.MigrationTaskQuery;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import java.sql.SQLException;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

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
    private static final boolean DESCENDING = false;
    public static final String OPERATOR = "operator";
    public static final String STATUS = "status";
    public static final String OLD_MHA = "old_mha";
    public static final String NEW_MHA = "new_mha";

    public MigrationTaskTblDao() throws SQLException {
        super(MigrationTaskTbl.class);
    }

    public Long insertWithReturnId(MigrationTaskTbl migrationTaskTbl) throws SQLException {
        KeyHolder keyHolder = new KeyHolder();
        insert(new DalHints(), keyHolder, migrationTaskTbl);
        return (Long) keyHolder.getKey();
    }
    

    public List<MigrationTaskTbl> queryByPage(MigrationTaskQuery query) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.atPage(query.getPageIndex(), query.getPageSize())
                .orderBy(ID, DESCENDING).and()
                .equalNullable(OPERATOR, query.getOperator(), Types.VARCHAR).and()
                .equalNullable(STATUS, query.getStatus(), Types.VARCHAR).and()
                .equalNullable(OLD_MHA, query.getOldMha(), Types.VARCHAR).and()
                .equalNullable(NEW_MHA, query.getNewMha(), Types.VARCHAR);

        return client.query(sqlBuilder, new DalHints());
    }

    public int count(MigrationTaskQuery query) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.selectCount().and()
                .equalNullable(OPERATOR, query.getOperator(), Types.VARCHAR).and()
                .equalNullable(STATUS, query.getStatus(), Types.VARCHAR).and()
                .equalNullable(OLD_MHA, query.getOldMha(), Types.VARCHAR).and()
                .equalNullable(NEW_MHA, query.getNewMha(), Types.VARCHAR);
        return client.count(sqlBuilder, new DalHints()).intValue();
    }

}
