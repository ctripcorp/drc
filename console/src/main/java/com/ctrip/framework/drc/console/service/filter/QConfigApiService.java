package com.ctrip.framework.drc.console.service.filter;

import com.ctrip.framework.drc.console.param.filter.QConfigQueryParam;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataResponse;

/**
 * Created by dengquanliang
 * 2023/4/24 14:35
 */
public interface QConfigApiService {

    QConfigDataResponse getQConfigData(QConfigQueryParam param);
}
