package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.aop.PossibleRemote;
import com.ctrip.xpipe.api.endpoint.Endpoint;

import java.util.Map;

public interface MySqlService {
    
    // forward by mha
    Map<String, String> getCreateTableStatements(String mha, String unionFilter, Endpoint endpoint);
    
    // forward by mha
    Integer getAutoIncrement(String mha,String sql,int index,Endpoint endpoint);
    
    // forward by mha
    String getDrcExecutedGtid(String mha);

    // forward by mha
    String getMhaExecutedGtid(String mha);
}
