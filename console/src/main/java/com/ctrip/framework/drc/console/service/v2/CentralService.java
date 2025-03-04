package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.param.MhaReplicatorEntity;
import com.ctrip.framework.drc.console.param.mysql.DdlHistoryEntity;
import com.ctrip.framework.drc.console.param.v2.security.MhaAccounts;

import java.sql.SQLException;
import java.util.List;

public interface CentralService {
    
    List<MhaTblV2> getMhaTblV2s(String dcName) throws SQLException;

    Integer insertDdlHistory(DdlHistoryEntity requestBody) throws SQLException;

    List<MhaDbReplicationDto> getMhaDbReplications(String dcName) throws SQLException;

    String getUuidInMetaDb(String mhaName, String ip, Integer port) throws SQLException;
    
    Integer correctMachineUuid(MachineTbl requestBody) throws SQLException;
    
    MhaAccounts getMhaAccounts(String mhaName) throws SQLException;

    Boolean updateMasterReplicatorIfChange(MhaReplicatorEntity requestBody)  throws SQLException;

    List<DcTbl> queryAllDcTbl() throws SQLException;

    List<MhaTblV2> queryAllMhaTblV2() throws SQLException;

    List<ResourceTbl> queryAllResourceTbl() throws SQLException;


}
