package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.param.mysql.QueryRecordsRequest;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.check.TableCheckVo;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by dengquanliang
 * 2023/8/11 15:48
 */
public interface MysqlServiceV2 {
    // forward by mha
    String getMhaExecutedGtid(String mha);

    // forward by mha
    String getMhaPurgedGtid(String mha);

    // query (sourceMhaName) delay monitor info in (mha)
    Long getDelayUpdateTime(String sourceMhaName, String mhaName);

    // route By mha
    Map<String, Object> preCheckMySqlConfig(String mha) ;

    // route By mha
    List<TableCheckVo> preCheckMySqlTables(String mha, String nameFilter);

    List<MySqlUtils.TableSchemaName> getMatchTable(String mhaName,String nameFilter);

    // nameFilters: split with ','
    List<MySqlUtils.TableSchemaName> getAnyMatchTable(String mhaName, String nameFilters);

    // route By mha
    List<String> queryDbsWithNameFilter(String mha, String nameFilter);

    // route By mha
    List<String> queryTablesWithNameFilter(String mha, String nameFilter);

    // route By mhaName
    Set<String> getCommonColumnIn(String mhaName, String namespace, String name);

    Set<String> getTablesWithoutColumn(String column, String namespace, String name, String mhaName);

    Long getCurrentTime(String mha);

    Map<String, Object> queryTableRecords(QueryRecordsRequest requestBody) throws Exception;

    List<String> getAllOnUpdateColumns(String mha, String db, String table);

    String getFirstUniqueIndex(String mha, String db, String table);
}
