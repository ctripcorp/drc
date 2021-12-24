package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.fetcher.resource.context.sql.SelectBuilder;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractTableSqlBuilder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;

import java.sql.SQLException;
import java.sql.Types;

/**
 * @author shb沈海波
 * @date 2020-08-11
 */
public class MachineTblDao extends AbstractDao<MachineTbl> {

    public MachineTblDao() throws SQLException {
        super(MachineTbl.class);
    }

    public MachineTbl queryByIpPort(String ip, int port) throws SQLException {
        DalHints hints = DalHints.createIfAbsent(null);
        SelectSqlBuilder sqlBuilder = (SelectSqlBuilder) new SelectSqlBuilder().equal("ip", ip, Types.VARCHAR)
                .and().equal("port", port, Types.INTEGER)
                .and().equal("deleted", 0, Types.TINYINT);
        MachineTbl machineTbl = client.queryFirst(sqlBuilder, hints);
        return machineTbl;
    }
}