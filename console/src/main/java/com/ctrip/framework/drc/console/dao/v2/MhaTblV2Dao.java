package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/5/25 12:04
 */
@Repository
public class MhaTblV2Dao extends AbstractDao<MhaTblV2> {

    public MhaTblV2Dao() throws SQLException {
        super(MhaTblV2.class);
    }
}
