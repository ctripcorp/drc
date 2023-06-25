package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.param.NameFilterSplitParam;
import com.ctrip.framework.drc.console.vo.api.MhaNameFilterVo;
import com.ctrip.framework.drc.console.vo.response.migrate.MhaDbMappingResult;
import com.ctrip.framework.drc.console.vo.response.migrate.MigrateResult;

import java.util.List;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/6/5 16:51
 */
public interface MetaMigrateService {

    int batchInsertRegions(List<String> regionNames) throws Exception;

    int batchUpdateDcRegions(Map<String, String> dcRegionMap) throws Exception;

    MigrateResult migrateMhaTbl() throws Exception;

    MigrateResult migrateMhaReplication() throws Exception;

    MigrateResult migrateApplierGroup() throws Exception;

    MigrateResult migrateApplier() throws Exception;

    MhaDbMappingResult checkMhaDbMapping() throws Exception;

    MigrateResult migrateMhaDbMapping() throws Exception;

    List<MhaNameFilterVo> checkMhaFilter() throws Exception;

    int splitNameFilter(List<NameFilterSplitParam> paramList) throws Exception;

    MigrateResult migrateColumnsFilter() throws Exception;

    MigrateResult migrateRowsFilter() throws Exception;

    MigrateResult migrateDbReplicationTbl() throws Exception;

    List<MhaNameFilterVo> checkNameMapping() throws Exception;

    MigrateResult splitNameFilterWithNameMapping() throws Exception;

    MigrateResult migrateMessengerGroup() throws Exception;

    MigrateResult migrateMessengerFilter() throws Exception;

    MigrateResult migrateDbReplicationFilterMapping() throws Exception;

}
