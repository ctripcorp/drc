package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/5/25 12:02
 */
@Repository
public class MhaDbMappingTblDao extends AbstractDao<MhaDbMappingTbl> {

    public MhaDbMappingTblDao() throws SQLException {
        super(MhaDbMappingTbl.class);
    }
}
