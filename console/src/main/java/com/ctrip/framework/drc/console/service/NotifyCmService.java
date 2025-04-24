package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.enums.HttpRequestEnum;
import com.ctrip.framework.drc.console.enums.DlockEnum;

import java.util.List;

/**
 * Created by shiruixin
 * 2025/4/16 16:31
 */
public interface NotifyCmService {
    void pushConfigToCM(List<String> mhaNames, DlockEnum operator, HttpRequestEnum httpRequestEnum) throws Exception;
    void pushConfigToCM(List<Long> mhaIds, String operator, HttpRequestEnum httpRequestEnum) throws Exception;
}
