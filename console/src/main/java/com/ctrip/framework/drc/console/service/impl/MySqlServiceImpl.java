package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.aop.PossibleRemote;
import com.ctrip.framework.drc.console.service.MySqlService;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * @ClassName MySqlServiceImpl
 * @Author haodongPan
 * @Date 2022/8/17 19:49
 * @Version: $
 */
@Service
public class MySqlServiceImpl implements MySqlService {
    
    @Override
    @PossibleRemote(path="/api/drc/v1/local/createTblStmts/query",excludeArguments = {"endpoint"})
    public Map<String, String> getCreateTableStatements(String mha, String unionFilter, Endpoint endpoint) {
        AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(unionFilter);
        return MySqlUtils.getDefaultCreateTblStmts(endpoint,aviatorRegexFilter);
    }

    @Override
    @PossibleRemote(path="/api/drc/v1/local/sql/integer/query",excludeArguments = {"endpoint"})
    public Integer getAutoIncrement(String mha, String sql, int index, Endpoint endpoint) {
        return MySqlUtils.getSqlResultInteger(endpoint, sql, index);
    }
}
