package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.param.v2.ResourceQueryParam;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.util.digester.ArrayStack;

import java.sql.SQLException;
import java.sql.Types;
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

    public List<ResourceTbl> queryByParam(ResourceQueryParam param) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        return new ArrayStack<>();
    }

}