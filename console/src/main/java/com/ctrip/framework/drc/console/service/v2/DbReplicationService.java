package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.param.v2.MqReplicationQuery;
import com.ctrip.framework.drc.console.vo.display.v2.DbReplicationVo;
import com.ctrip.framework.drc.console.vo.request.MqReplicationQueryDto;
import com.ctrip.framework.drc.core.http.PageResult;

import java.sql.SQLException;

/**
 * Created by shiruixin
 * 2024/8/27 11:37
 */
public interface DbReplicationService {

    PageResult<DbReplicationVo> queryMqReplicationsByPage(MqReplicationQueryDto queryDto) throws SQLException;

    PageResult<DbReplicationTbl> queryByPage(MqReplicationQuery query);
}
