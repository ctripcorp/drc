package com.ctrip.framework.drc.console.service.v2;

/**
 * Created by dengquanliang
 * 2023/7/5 14:59
 */
public interface DrcDoubleWriteService {
    void buildMha(Long mhaGroupId) throws Exception;

    void buildApplierGroup(Long applierGroupId) throws Exception;

    void buildAppliers(Long applierGroupId) throws Exception;

    void buildMhaAndDbReplication(Long applierGroupId) throws Exception;

    void insertRowsFilter(Long applierGroupId, Long dataMediaId, Long rowsFilterId) throws Exception;

    void deleteRowsFilter(Long rowsFilterMappingId) throws Exception;

    void insertOrUpdateColumnsFilter(Long dataMediaId) throws Exception;

    void deleteColumnsFilter(Long dataMediaId) throws Exception;

}
