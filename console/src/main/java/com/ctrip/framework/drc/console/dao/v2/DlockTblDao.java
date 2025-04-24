package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.DlockTbl;
import com.ctrip.platform.dal.dao.DalHints;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by shiruixin
 * 2025/4/21 17:08
 */
@Repository
public class DlockTblDao extends AbstractDao<DlockTbl> {
    private static final String MHA_NAME = "mha_name";
    private static final String LOCK_NAME = "lock_name";
    private static final String DATACHANGE_LASTTIME = "datachange_lasttime";

    public DlockTblDao() throws SQLException {
        super(DlockTbl.class);
    }

    public int deleteByMhaNames(List<DlockTbl> tbls, int expireTime) throws SQLException {
        if (CollectionUtils.isEmpty(tbls)) {
            return 0;
        }
        String mhaPattern = tbls.stream().map(dlockTbl -> "'" + dlockTbl.getMhaName() + "'").collect(Collectors.joining(","));
        String sql = String.format("%s in (%s) AND %s = ? AND %s < NOW() - INTERVAL %d MINUTE",
                MHA_NAME, mhaPattern, LOCK_NAME, DATACHANGE_LASTTIME, expireTime);
        return client.delete(sql, new DalHints(), tbls.getFirst().getLockName());

    }

    public int deleteByMhaNames(List<DlockTbl> tbls) throws SQLException {
        if (CollectionUtils.isEmpty(tbls)) {
            return 0;
        }
        String mhaPattern = tbls.stream().map(dlockTbl -> "'" + dlockTbl.getMhaName() + "'").collect(Collectors.joining(","));
        String sql = String.format("%s in (%s) AND %s = ?",
                MHA_NAME, mhaPattern, LOCK_NAME);
        return client.delete(sql, new DalHints(), tbls.getFirst().getLockName());

    }

}
