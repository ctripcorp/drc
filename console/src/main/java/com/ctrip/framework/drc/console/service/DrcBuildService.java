package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dto.MessengerMetaDto;
import com.ctrip.framework.drc.console.dto.MetaProposalDto;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.DrcBuildPreCheckVo;
import com.ctrip.framework.drc.console.vo.TableCheckVo;
import com.ctrip.framework.drc.core.http.ApiResult;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;


public interface DrcBuildService {
    String submitConfig(MetaProposalDto metaProposalDto) throws Exception;

    String submitConfig(MessengerMetaDto dto) throws Exception;
    
    DrcBuildPreCheckVo preCheckBeforeBuild(MetaProposalDto metaProposalDto) throws SQLException;
    
    DrcBuildPreCheckVo preCheckBeReplicatorIps(MessengerMetaDto dto) throws SQLException;

    // route By mha
    Map<String, Object> preCheckMySqlConfig(String mha) ;

    // route By mha
    List<TableCheckVo> preCheckMySqlTables(String mha, String nameFilter);

    // route By mhaName
    List<MySqlUtils.TableSchemaName> getMatchTable(String namespace, String name, String mhaName, Integer type);

    // route By mhaName
    Set<String>  getCommonColumnInDataMedias(String mhaName, String namespace, String name);
    
    ApiResult getOrBuildSimplexDrc(String srcMha, String destMha) throws SQLException;
}
