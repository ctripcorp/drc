package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.param.NameFilterSplitParam;
import com.ctrip.framework.drc.console.vo.api.MhaNameFilterVo;

import java.util.List;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/6/5 16:51
 */
public interface MetaMigrateService {

    int batchInsertRegions(List<String> regionNames) throws Exception;

    int batchUpdateDcRegions(Map<String, String> dcRegionMap) throws Exception;

    int migrateMhaTbl() throws Exception;

    int migrateMhaReplication() throws Exception;

    int migrateApplierGroup() throws Exception;

    int migrateApplier() throws Exception;

    String checkMhaDbMapping() throws Exception;

    String migrateMhaDbMapping() throws Exception;

    List<MhaNameFilterVo> checkMhaFilter() throws Exception;

    int splitNameFilter(List<NameFilterSplitParam> paramList) throws Exception;

    int migrateColumnsFilter() throws Exception;

    int migrateDbReplicationTbl() throws Exception;

    List<String> checkNameMapping() throws Exception;

    int splitNameFilterWithNameMapping() throws Exception;

    int migrateMessengerGroup() throws Exception;

    int migrateMessengerFilter() throws Exception;

    int migrateDbReplicationFilterMapping() throws Exception;

}
