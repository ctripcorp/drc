package com.ctrip.framework.drc.console.service;

import com.ctrip.xpipe.api.endpoint.Endpoint;

import java.util.Map;

public interface MySqlService {
    
    Map<String, String> getCreateTableStatements(String mha, String unionFilter, Endpoint endpoint);

    Integer getAutoIncrement(String mha,String sql,int index,Endpoint endpoint);
}
