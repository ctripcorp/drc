package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.core.http.ApiResult;
import java.sql.SQLException;

public interface DbMetaCorrectService {

    boolean updateMasterReplicatorIfChange(String mhaName, String newIp)  throws SQLException;

    void mhaInstancesChange(MhaInstanceGroupDto mhaInstanceGroupDto, MhaTblV2 mhaTblV2) throws Exception;

    ApiResult mhaMasterDbChange(String mhaName, String ip, int port);

}
