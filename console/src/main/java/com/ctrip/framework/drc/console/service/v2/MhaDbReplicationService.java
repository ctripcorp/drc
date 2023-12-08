package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/7/31 16:48
 */
public interface MhaDbReplicationService {
    /**
     * query mhaDbReplication by conditions
     *
     * @param srcMhaName src mha
     * @param dstMhaName dst mha
     * @param dbNames    related db names. query all if empty or null
     */
    List<MhaDbReplicationDto> queryByMha(String srcMhaName, String dstMhaName, List<String> dbNames);

    List<MhaDbReplicationDto> queryMqByMha(String srcMhaName, List<String> dbNames);

    List<MhaDbReplicationDto> queryByDcName(String srcDcName, ReplicationTypeEnum typeEnum);

    void refreshMhaReplication();

    void maintainMhaDbReplication(List<DbReplicationTbl> dbReplicationTbls) throws SQLException;
}
