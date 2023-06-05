package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierTblV2;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/5/25 11:57
 */
@Repository
public class ApplierTblV2Dao extends AbstractDao<ApplierTblV2> {

    public ApplierTblV2Dao() throws SQLException {
        super(ApplierTblV2.class);
    }
}
