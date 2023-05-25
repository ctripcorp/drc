package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.ColumnsFilterTblV2;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/5/25 11:59
 */
@Repository
public class ColumnsFilterTblV2Dao extends AbstractDao<ColumnsFilterTblV2> {

    public ColumnsFilterTblV2Dao() throws SQLException {
        super(ColumnsFilterTblV2.class);
    }
}
