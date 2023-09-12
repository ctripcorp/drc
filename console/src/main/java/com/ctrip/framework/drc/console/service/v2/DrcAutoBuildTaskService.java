package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildParam;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildReq;
import com.ctrip.framework.drc.console.vo.display.v2.MhaReplicationPreviewDto;

import java.util.List;


public interface DrcAutoBuildTaskService {
    List<MhaReplicationPreviewDto> previewAutoBuildOptions(DrcAutoBuildReq req);

    List<DrcAutoBuildParam> getDrcBuildParam(DrcAutoBuildReq req);

    List<String> getRegionOptions(DrcAutoBuildReq req);

    void autoBuildDrc(DrcAutoBuildReq req) throws Exception;
}
