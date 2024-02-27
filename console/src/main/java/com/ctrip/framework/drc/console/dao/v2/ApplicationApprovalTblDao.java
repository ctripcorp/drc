package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplicationApprovalTbl;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengquanliang
 * 2024/1/31 16:40
 */
@Repository
public class ApplicationApprovalTblDao extends AbstractDao<ApplicationApprovalTbl> {

    private static final String APPLICATION_FORM_ID = "application_form_id";

    public ApplicationApprovalTblDao() throws SQLException {
        super(ApplicationApprovalTbl.class);
    }

    public List<ApplicationApprovalTbl> queryByApplicationFormIds(List<Long> applicationFormIds) throws SQLException {
        if (CollectionUtils.isEmpty(applicationFormIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().in(APPLICATION_FORM_ID, applicationFormIds, Types.BIGINT);
        return queryList(sqlBuilder);
    }

    public ApplicationApprovalTbl queryByApplicationFormId(Long applicationFormId) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(APPLICATION_FORM_ID, applicationFormId, Types.BIGINT);
        return queryOne(sqlBuilder);
    }
}
