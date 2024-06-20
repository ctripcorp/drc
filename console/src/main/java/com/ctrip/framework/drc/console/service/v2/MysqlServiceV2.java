package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.aop.forward.PossibleRemote;
import com.ctrip.framework.drc.console.param.mysql.DbFilterReq;
import com.ctrip.framework.drc.console.param.mysql.DrcDbMonitorTableCreateReq;
import com.ctrip.framework.drc.console.param.mysql.MysqlWriteEntity;
import com.ctrip.framework.drc.console.param.mysql.QueryRecordsRequest;
import com.ctrip.framework.drc.console.param.v2.security.MhaAccounts;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.check.TableCheckVo;
import com.ctrip.framework.drc.console.vo.check.v2.AutoIncrementVo;
import com.ctrip.framework.drc.core.monitor.operator.StatementExecutorResult;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Created by dengquanliang
 * 2023/8/11 15:48
 */
public interface MysqlServiceV2 {
    // forward by mha
    String getMhaExecutedGtid(String mha);

    // forward by mha
    String getMhaPurgedGtid(String mha);

    String getMhaAppliedGtid(String mha);

    Map<String /*dbName*/, String /*applied gtid*/> getMhaDbAppliedGtid(String mha);

    // query (sourceMhaName) delay monitor info in (mha)
    Long getDelayUpdateTime(String sourceMhaName, String mhaName);

    Map<String /*dbName*/, Long /*time*/> getDbDelayUpdateTime(String sourceMhaName, String mhaName, List<String> dbNames);

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

    /**
     * key: tableName, values: columns
     */
    Map<String, Set<String>> getTableColumns(DbFilterReq requestBody);

    Set<String> getTablesWithoutColumn(String column, String namespace, String name, String mhaName);

    Long getCurrentTime(String mha);

    Map<String, Object> queryTableRecords(QueryRecordsRequest requestBody) throws Exception;

    List<String> getAllOnUpdateColumns(String mha, String db, String table);

    String getFirstUniqueIndex(String mha, String db, String table);

    List<String> getUniqueIndex(String mha, String db, String table);

    StatementExecutorResult write(MysqlWriteEntity requestBody);

    Boolean createDrcMonitorDbTable(DrcDbMonitorTableCreateReq requestBody);

    AutoIncrementVo getAutoIncrementAndOffset(String mha);
    
    Pair<Boolean,String> checkAccountsPrivileges(String mha, MhaAccounts oldAccounts, MhaAccounts newAccounts);

    @PossibleRemote(path = "/api/drc/v2/mysql/accountPrivileges")
    String checkAccountPrivileges(String mha, String account, String pwd);
}
