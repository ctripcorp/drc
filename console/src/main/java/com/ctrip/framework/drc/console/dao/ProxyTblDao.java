package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ProxyTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
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

    public ProxyTblDao() throws SQLException {
        super(ProxyTbl.class);
    }

    public List<ProxyTbl> queryByDcId(Long dcId, Integer deleted) throws SQLException {
        ProxyTbl sample = new ProxyTbl();
        sample.setDcId(dcId);
        sample.setDeleted(deleted);
        return this.queryBy(sample);
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
