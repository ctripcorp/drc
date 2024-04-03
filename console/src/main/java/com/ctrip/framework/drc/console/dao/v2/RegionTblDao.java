package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.RegionTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/25 12:05
 */
@Repository
public class RegionTblDao extends AbstractDao<RegionTbl> {

    private static final String REGION_NAME = "region_name";

    public RegionTblDao() throws SQLException {
        super(RegionTbl.class);
    }

    @Override
    public List<RegionTbl> queryAll() throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.selectAll().orderBy("id", true);
        return client.query(sqlBuilder, new DalHints());
    }

    public Long upsert(String regionName) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(REGION_NAME, regionName, Types.VARCHAR);
        RegionTbl regionTbl = queryOne(sqlBuilder);
        if (regionTbl == null) {
            RegionTbl newRegionTbl = new RegionTbl();
            newRegionTbl.setRegionName(regionName);
            return insertWithReturnId(newRegionTbl);
        } else {
            if (regionTbl.getDeleted().equals(BooleanEnum.TRUE.getCode())) {
                regionTbl.setDeleted(BooleanEnum.FALSE.getCode());
                update(regionTbl);
            }
            return regionTbl.getId();
        }
    }
}
