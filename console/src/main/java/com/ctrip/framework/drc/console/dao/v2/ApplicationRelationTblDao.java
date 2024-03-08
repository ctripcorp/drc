package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplicationRelationTbl;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Created by dengquanliang
 * 2024/2/26 15:09
 */
@Repository
public class ApplicationRelationTblDao extends AbstractDao<ApplicationRelationTbl> {

    private static final String APPLICATION_FORM_ID = "application_form_id";

    public ApplicationRelationTblDao() throws SQLException {
        super(ApplicationRelationTbl.class);
    }

    public List<ApplicationRelationTbl> queryByApplicationFormId(Long applicationFormId) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(APPLICATION_FORM_ID, applicationFormId, Types.BIGINT);
        return queryList(sqlBuilder);
    }
}
