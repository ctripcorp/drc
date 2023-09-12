package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.unidal.tuple.Triple;

import java.sql.SQLException;
import java.util.List;

public interface DbMetaCorrectService {

    boolean updateMasterReplicatorIfChange(String mhaName, String newIp)  throws SQLException;

    void mhaInstancesChange(MhaInstanceGroupDto mhaInstanceGroupDto, MhaTblV2 mhaTblV2) throws Exception;

    void mhaInstancesChange(List<MachineTbl> machinesInDba, MhaTblV2 mhaTblV2) throws Exception;

    ApiResult mhaMasterDbChange(String mhaName, String ip, int port);

}
