package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaMappingTbl;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/4/23 20:05
 */
@Repository
public class RowsFilterMetaMappingTblDao extends AbstractDao<RowsFilterMetaMappingTbl> {

    public RowsFilterMetaMappingTblDao(Class<RowsFilterMetaMappingTbl> clazz) throws SQLException {
        super(clazz);
    }
}
