package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.DataMediaPairTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

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
}

