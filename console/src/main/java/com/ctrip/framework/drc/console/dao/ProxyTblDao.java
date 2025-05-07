package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ProxyTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.sqlbuilder.MatchPattern;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2021-05-12
 */
@Repository
public class ProxyTblDao extends AbstractDao<ProxyTbl> {

    private static final String URI = "uri";
    private static final String DC_ID = "dc_id";
    private static final String DATACHANGE_LASTTIME = "datachange_lasttime";
    private static final boolean DESCENDING = false;

    public ProxyTblDao() throws SQLException {
        super(ProxyTbl.class);
    }

    public List<ProxyTbl> queryByDcId(Long dcId, Integer deleted) throws SQLException {
        ProxyTbl sample = new ProxyTbl();
        sample.setDcId(dcId);
        sample.setDeleted(deleted);
        return this.queryBy(sample);
    }

    public List<ProxyTbl> queryByDcId(Long dcId, String prefix, String suffix) throws SQLException {
        SelectSqlBuilder builder = initSqlBuilder();
        builder.and().equal(DC_ID, dcId, Types.BIGINT)
                .and().like(URI, prefix, MatchPattern.BEGIN_WITH, Types.VARCHAR)
                .and().like(URI, suffix, MatchPattern.END_WITH, Types.VARCHAR);
        builder.orderBy(DATACHANGE_LASTTIME, DESCENDING);
        return queryList(builder);
    }

    public List<ProxyTbl> queryByPrefix(String prefix, String suffix) throws SQLException {
        SelectSqlBuilder builder = initSqlBuilder();
        builder.and().like(URI, prefix, MatchPattern.BEGIN_WITH, Types.VARCHAR)
                .and().like(URI, suffix, MatchPattern.END_WITH, Types.VARCHAR);
        return queryList(builder);
    }

    public ProxyTbl queryByUri(String uri) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(URI, uri, Types.VARCHAR);
        return queryOne(sqlBuilder);
    }

    public void upsert(ProxyTbl pojo) throws SQLException {
        ProxyTbl proxyTbl = queryAll().stream().filter(p -> pojo.getUri().equalsIgnoreCase(p.getUri())).findFirst().orElse(null);
        if (null == proxyTbl) {
            if (BooleanEnum.FALSE.getCode().equals(pojo.getDeleted())) {
                insert(pojo);
            }
        } else if (!proxyTbl.getDcId().equals(pojo.getDcId()) || !proxyTbl.getActive().equals(pojo.getActive())
                || !proxyTbl.getMonitorActive().equals(pojo.getMonitorActive()) && !proxyTbl.getDeleted().equals(pojo.getDeleted())) {
            update(pojo);
        }
    }
}
