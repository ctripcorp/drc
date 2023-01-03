package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ColumnsFilterTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * Created by jixinwang on 2022/12/30
 */
@Repository
public class ColumnsFilterTblDao extends AbstractDao<ColumnsFilterTbl> {

    public ColumnsFilterTblDao() throws SQLException {
        super(ColumnsFilterTbl.class);
    }

    public Long insertReturnPk(ColumnsFilterTbl columnsFilterTbl) throws SQLException{
        KeyHolder keyHolder = new KeyHolder();
        insert(new DalHints(), keyHolder, columnsFilterTbl);
        return (Long) keyHolder.getKey();
    }
}
