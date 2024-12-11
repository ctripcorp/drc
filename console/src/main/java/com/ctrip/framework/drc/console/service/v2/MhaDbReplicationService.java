package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.entity.v3.MhaDbReplicationTbl;
import com.ctrip.framework.drc.console.dto.v2.MhaDbDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.vo.request.MhaDbReplicationQueryDto;
import com.ctrip.framework.drc.core.http.PageResult;

import java.sql.SQLException;
import java.util.List;

public interface MhaDbReplicationService {
    /**
     * query mhaDbReplication by conditions
     * @param srcMhaName src mha
     * @param dstMhaName dst mha
     * @param dbNames    related db names. query all if empty or null
     */
    List<MhaDbReplicationDto> queryByMha(String srcMhaName, String dstMhaName, List<String> dbNames);

    List<MhaDbReplicationTbl> queryBySrcMha(String srcMhaName) throws SQLException;

    List<MhaDbReplicationDto> queryMqByMha(String srcMhaName, List<String> dbNames);

    List<MhaDbReplicationDto> queryByDcName(String srcDcName, ReplicationTypeEnum typeEnum);

    PageResult<MhaDbReplicationDto>  query(MhaDbReplicationQueryDto queryDto);


    void refreshMhaReplication();

    void maintainMhaDbReplication(List<DbReplicationTbl> dbReplicationTbls) throws SQLException;
    void maintainMhaDbReplication(String srcMhaName, String dstMhaName, List<String> dbNames) throws SQLException;
    void maintainMhaDbReplicationForMq(String srcMhaName, List<String> dbNames) throws SQLException;
    void offlineMhaDbReplication(String srcMhaName, String dstMhaName);

    /**
     * after delete dbReplicationTbls
     */
    List<MhaDbReplicationTbl> offlineMhaDbReplication(List<DbReplicationTbl> dbReplicationTbls) throws SQLException;

    void offlineMhaDbReplicationAndApplierV3(List<DbReplicationTbl> dbReplicationTbls) throws SQLException;

    boolean isDbReplicationExist(Long mhaId,List<String> dbs) throws SQLException;

    List<MhaTblV2> getReplicationRelatedMha(String db, String table) throws SQLException;

    List<MhaDbDelayInfoDto> getReplicationDelays(List<Long> replicationIds);

    List<MhaDbReplicationDto> queryByDbNames(List<String> dbNames, ReplicationTypeEnum typeEnum);

    List<MhaDbReplicationDto> queryByDbNamesAndMhaNames(List<String> dbNames, List<String> relateMhas, ReplicationTypeEnum typeEnum);
}
