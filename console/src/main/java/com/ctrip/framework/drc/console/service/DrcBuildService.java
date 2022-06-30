package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dto.MetaProposalDto;
import com.ctrip.framework.drc.console.vo.DrcBuildPreCheckVo;
import com.ctrip.framework.drc.console.vo.TableCheckVo;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;


public interface DrcBuildService {
    String submitConfig(MetaProposalDto metaProposalDto) throws Exception;
    
    DrcBuildPreCheckVo preCheckBeforeBuild(MetaProposalDto metaProposalDto) throws SQLException;

    Map<String, Object> preCheckMySqlConfig(String mha) ;

    List<TableCheckVo> preCheckMySqlTables(String mha, String nameFilter);
    
}
