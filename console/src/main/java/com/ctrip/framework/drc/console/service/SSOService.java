package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.service.ops.AppNode;

import java.net.UnknownHostException;
import java.util.List;

public interface SSOService {
    ApiResult notifyOtherMachine(Boolean isOpen) throws UnknownHostException;
    
    ApiResult setDegradeSwitch(Boolean isOpen);
    
    List<AppNode> getAppNodes();

    ApiResult degradeAllServer(Boolean isOpen);
}
