package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildReq;
import com.ctrip.framework.drc.console.vo.display.v2.MhaReplicationPreviewDto;

import java.sql.SQLException;
import java.util.List;


public interface DrcAutoBuildTaskService {
    List<MhaReplicationPreviewDto> previewAutoBuildOptions(String dalClusterName, String srcRegionName, String dstRegionName);

    void autoBuildDrc(DrcAutoBuildReq param) throws Exception;
}
