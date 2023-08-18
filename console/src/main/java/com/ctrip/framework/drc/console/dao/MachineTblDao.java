package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

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

    public List<MachineTbl> queryByMhaId(Long mhaId, Integer deleted) throws SQLException{
        if (null == mhaId ) {
            throw new IllegalArgumentException("build sql: queryMachineByMhaId, but id is null" );
        }
        SelectSqlBuilder builder = new SelectSqlBuilder();
        builder.selectAll().equal("mha_id", mhaId, Types.BIGINT,false)
                .and().equal("deleted", deleted, Types.TINYINT, false);
        return client.query(builder, new DalHints());
    }

    public int[] batchLogicalDelete(List<MachineTbl> deleteMachines) throws SQLException{
        return this.batchUpdate(deleteMachines);
    }
}