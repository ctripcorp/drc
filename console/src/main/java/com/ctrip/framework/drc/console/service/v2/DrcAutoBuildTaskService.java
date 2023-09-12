package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildReq;

import java.sql.SQLException;


public interface DrcAutoBuildTaskService {
    void autoBuildDrc(DrcAutoBuildReq param) throws Exception;
}
