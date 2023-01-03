package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ColumnsFilterMappingTbl;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * Created by jixinwang on 2022/12/30
 */
@Repository
public class ColumnsFilterMappingTblDao extends AbstractDao<ColumnsFilterMappingTbl> {

    public ColumnsFilterMappingTblDao() throws SQLException {
        super(ColumnsFilterMappingTbl.class);
    }
}
