package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.DataMediaPairTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.util.List;

/**
 * @ClassName DataMediaPairTblDao
 * @Author haodongPan
 * @Date 2022/10/8 14:26
 * @Version: $
 */
@Repository
public class DataMediaPairTblDao extends AbstractDao<DataMediaPairTbl> {
    
    public DataMediaPairTblDao() throws SQLException {
        super(DataMediaPairTbl.class);
    }

    public List<DataMediaPairTbl> queryByGroupId(Long messengerGroupId) throws SQLException {
        if (null == messengerGroupId) {
            throw new IllegalArgumentException("query DataMediaPairTbls By GroupId but messengerGroupId is null");
        }
        DataMediaPairTbl sample = new DataMediaPairTbl();
        sample.setGroupId(messengerGroupId);
        sample.setDeleted(BooleanEnum.FALSE.getCode());
        return queryBy(sample);
    }

    public Long insertReturnKey(DataMediaPairTbl dataMediaPairTbl) throws SQLException {
        KeyHolder keyHolder = new KeyHolder();
        insert(new DalHints(), keyHolder, dataMediaPairTbl);
        return (Long) keyHolder.getKey();
    }
}

