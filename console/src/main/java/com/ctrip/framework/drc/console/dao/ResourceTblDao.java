package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.param.v2.ResourceQueryParam;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.util.digester.ArrayStack;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2020-08-11
 */
public class ResourceTblDao extends AbstractDao<ResourceTbl> {

    private static final String IP = "ip";
    private static final String TYPE = "type";
    private static final String DC_ID = "dc_id";
    private static final String TAG = "tag";
    private static final String ACTIVE = "active";
    private static final String DELETED = "deleted";

    public ResourceTblDao() throws SQLException {
        super(ResourceTbl.class);
    }

    public ResourceTbl queryByIp(String ip) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.equal(IP, ip, Types.VARCHAR);
        return queryOne(sqlBuilder);
    }

    public List<ResourceTbl> queryByIps(List<String> ips) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().inNullable(IP, ips, Types.VARCHAR);
        return queryList(sqlBuilder);
    }

    public List<ResourceTbl> queryByType(int type) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(TYPE, type, Types.TINYINT);
        return queryList(sqlBuilder);
    }

    public List<ResourceTbl> queryByParam(ResourceQueryParam param) throws SQLException {
        SelectSqlBuilder sqlbuilder = buildSqlBuild(param);
        sqlbuilder.selectCount();
        int count = client.count(sqlbuilder, new DalHints()).intValue();
        param.getPageReq().setTotalCount(count);

        sqlbuilder = buildSqlBuild(param);
        sqlbuilder.selectAll().atPage(param.getPageReq().getPageIndex(), param.getPageReq().getPageSize());
        return queryList(sqlbuilder);
    }

    public List<ResourceTbl> queryByDcAndTag(List<Long> dcIds, String tag, int type) throws SQLException {
        SelectSqlBuilder sqlbuilder = initSqlBuilder();
        sqlbuilder.and().inNullable(DC_ID, dcIds, Types.BIGINT)
                .and().equal(TAG, tag, Types.VARCHAR)
                .and().equal(TYPE, type, Types.TINYINT);
        return queryList(sqlbuilder);
    }

    private SelectSqlBuilder buildSqlBuild(ResourceQueryParam param) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        if (StringUtils.isNotBlank(param.getIp())) {
            sqlBuilder.and().equal(IP, param.getIp(), Types.VARCHAR);
        }
        if (param.getType() != null && param.getType() > -1) {
            sqlBuilder.and().equal(TYPE, param.getType(), Types.TINYINT);
        }
        if (StringUtils.isNotBlank(param.getTag())) {
            sqlBuilder.and().equal(TAG, param.getTag(), Types.VARCHAR);
        }
        if (param.getDcId() != null && param.getDcId() > 0L) {
            sqlBuilder.and().equal(DC_ID, param.getDcId(), Types.BIGINT);
        }
        if (param.getActive() != null && param.getActive() > -1) {
            sqlBuilder.and().equal(ACTIVE, param.getActive(), Types.TINYINT);
        }
        return sqlBuilder;
    }

}