package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dto.MetaProposalDto;
import com.ctrip.framework.drc.console.vo.DrcBuildPreCheckVo;

import java.sql.SQLException;


public interface DrcBuildService {
    String submitConfig(MetaProposalDto metaProposalDto) throws Exception;
    
    DrcBuildPreCheckVo preCheckBeforeBuild(MetaProposalDto metaProposalDto) throws SQLException;
}
