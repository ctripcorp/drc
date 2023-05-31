package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.RouteTbl;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

@Repository
public class RouteTblDao extends AbstractDao<RouteTbl> {

    public RouteTblDao() throws SQLException {
        super(RouteTbl.class);
    }

}