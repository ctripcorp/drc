package com.ctrip.framework.drc.console.service.v2;

/**
 * Created by dengquanliang
 * 2023/7/5 14:59
 */
public interface DrcDoubleWriteService {
    void buildMha(Long mhaGroupId) throws Exception;

    void configureMhaReplication(Long applierGroupId) throws Exception;

    void deleteMhaReplicationConfig(Long mhaId0, Long mhaId1) throws Exception;

    void insertRowsFilter(Long applierGroupId, Long dataMediaId, Long rowsFilterId) throws Exception;

    void deleteRowsFilter(Long rowsFilterMappingId) throws Exception;

    void insertColumnsFilter(Long dataMediaId) throws Exception;

    void deleteColumnsFilter(Long dataMediaId) throws Exception;

    void buildMhaForMq(Long mhaId) throws Exception;

    void insertDbReplicationForMq(Long dataMediaPairId) throws Exception;

    void deleteDbReplicationForMq(Long dataMediaPairId) throws Exception;

    void buildDbReplicationForMq(Long mhaId) throws Exception;

    void deleteMqConfig(Long mhaId) throws Exception;

    void switchMonitor(Long mhaId) throws Exception;
}
