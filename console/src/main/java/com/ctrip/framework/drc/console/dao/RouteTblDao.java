package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.RouteTbl;

import java.sql.SQLException;

public class RouteTblDao extends AbstractDao<RouteTbl> {

    public RouteTblDao() throws SQLException {
        super(RouteTbl.class);
    }

}