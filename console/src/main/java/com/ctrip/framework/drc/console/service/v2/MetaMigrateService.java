package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.vo.api.MhaNameFilterVo;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/6/5 16:51
 */
public interface MetaMigrateService {

    int migrateMhaTbl() throws Exception;

    int migrateMhaReplication() throws Exception;

    int migrateApplierGroup() throws Exception;

    int migrateApplier() throws Exception;

    String checkMhaDbMapping() throws Exception;

    String migrateMhaDbMapping() throws Exception;

    List<MhaNameFilterVo> checkMhaFilter() throws Exception;

    int migrateColumnsFilter() throws Exception;

    int migrateDbReplicationTbl() throws Exception;

}
