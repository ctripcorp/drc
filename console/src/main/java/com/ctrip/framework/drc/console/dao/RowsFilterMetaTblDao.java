package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaTbl;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/4/23 19:47
 */
@Repository
public class RowsFilterMetaTblDao extends AbstractDao<RowsFilterMetaTbl> {

    public RowsFilterMetaTblDao(Class<RowsFilterMetaTbl> clazz) throws SQLException {
        super(clazz);
    }

}
