package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.vo.check.TableCheckVo;
import com.ctrip.xpipe.api.endpoint.Endpoint;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by dengquanliang
 * 2023/8/11 15:48
 */
public interface MysqlServiceV2 {
    // forward by mha
    Map<String, String> getCreateTableStatements(String mha, String unionFilter, Endpoint endpoint);

    // forward by mha
    Integer getAutoIncrement(String mha,String sql,int index,Endpoint endpoint);

    // forward by mha
    String getDrcExecutedGtid(String mha);

    // forward by mha
    String getMhaExecutedGtid(String mha);

    // forward by mha
    String getMhaPurgedGtid(String mha);

    // route By mha
    Map<String, Object> preCheckMySqlConfig(String mha) ;

    // route By mha
    List<TableCheckVo> preCheckMySqlTables(String mha, String nameFilter);

    // route By mha
    List<String> queryDbsWithNameFilter(String mha, String nameFilter);

    // route By mha
    List<String> queryTablesWithNameFilter(String mha, String nameFilter);

    // route By mhaName
    Set<String> getCommonColumnIn(String mhaName, String namespace, String name);
}
