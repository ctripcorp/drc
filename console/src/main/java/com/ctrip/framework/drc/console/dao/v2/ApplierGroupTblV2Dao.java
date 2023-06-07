package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierGroupTblV2;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/5/25 11:54
 */
@Repository
public class ApplierGroupTblV2Dao extends AbstractDao<ApplierGroupTblV2> {

    public ApplierGroupTblV2Dao() throws SQLException {
        super(ApplierGroupTblV2.class);
    }
}
