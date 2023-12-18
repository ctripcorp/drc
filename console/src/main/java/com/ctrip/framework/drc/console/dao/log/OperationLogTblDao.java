package com.ctrip.framework.drc.console.dao.log;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.log.entity.OperationLogTbl;
import com.ctrip.framework.drc.console.param.log.OperationLogQueryParam;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

/**
 * @ClassName OperationLogTblDao
 * @Author haodongPan
 * @Date 2023/12/6 15:39
 * @Version: $
 */
@Repository
public class OperationLogTblDao extends AbstractDao<OperationLogTbl> {
    
    public static final String ID = "id";
    public static final String TYPE = "type";
    public static final String ATTR = "attr";
    public static final String OPERATOR = "operator";
    public static final String FAIL = "fail";
    public static final String CREATE_TIME = "create_time";
    

    public OperationLogTblDao() throws SQLException {
        super(OperationLogTbl.class);
    }

    public List<OperationLogTbl> queryByParam(OperationLogQueryParam param) throws SQLException {
        SelectSqlBuilder sqlBuilder = buildSqlBuilder(param);
        sqlBuilder.selectAll().atPage(param.getPageReq().getPageIndex(), param.getPageReq().getPageSize()).orderBy(CREATE_TIME, false);
        return queryList(sqlBuilder);
    }

    public Long countBy(OperationLogQueryParam param) throws SQLException {
        SelectSqlBuilder selectSqlBuilder = buildSqlBuilder(param);
        selectSqlBuilder.selectCount();
        return client.count(selectSqlBuilder, new DalHints()).longValue();
    }
    
    
    private SelectSqlBuilder buildSqlBuilder(OperationLogQueryParam param) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        if (StringUtils.isNotEmpty(param.getType())) {
            sqlBuilder.and().equal(TYPE, param.getType(), Types.VARCHAR);
        }
        if (StringUtils.isNotEmpty(param.getAttr())) {
            sqlBuilder.and().equal(ATTR, param.getAttr(), Types.VARCHAR);
        }
        if (StringUtils.isNotEmpty(param.getOperator())) {
            sqlBuilder.and().equal(OPERATOR, param.getOperator(), Types.VARCHAR);
        }
        if (param.getFail() != null) {
            sqlBuilder.and().equal(FAIL, param.getFail(), Types.INTEGER);
        }
        if (param.getBeginCreateTime() != null && param.getBeginCreateTime() > 0L) {
            sqlBuilder.and().greaterThanEquals(CREATE_TIME, new Timestamp(param.getBeginCreateTime()), Types.TIMESTAMP);
        }
        if (param.getEndCreatTime() != null && param.getEndCreatTime() > 0L) {
            sqlBuilder.and().lessThan(CREATE_TIME, new Timestamp(param.getEndCreatTime()),Types.TIMESTAMP);
        }
        return sqlBuilder;
    }

    
}
