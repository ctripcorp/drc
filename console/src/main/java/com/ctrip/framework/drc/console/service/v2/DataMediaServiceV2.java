package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dto.v2.DbReplicationDto;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/26 15:47
 */
public interface DataMediaServiceV2 {
    DataMediaConfig generateConfig(List<DbReplicationDto> dbReplicationDtos) throws SQLException;

    DataMediaConfig generateConfigFast(List<DbReplicationDto> dbReplicationDtos) throws SQLException;
}
