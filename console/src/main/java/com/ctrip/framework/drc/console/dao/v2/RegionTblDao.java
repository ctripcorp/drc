package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.RegionTbl;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/5/25 12:05
 */
@Repository
public class RegionTblDao extends AbstractDao<RegionTbl> {

    public RegionTblDao() throws SQLException {
        super(RegionTbl.class);
    }
}
