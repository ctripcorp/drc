package com.ctrip.framework.drc.console.service.filter;

import com.ctrip.framework.drc.console.param.filter.QConfigBatchUpdateParam;
import com.ctrip.framework.drc.console.param.filter.QConfigQueryParam;
import com.ctrip.framework.drc.console.param.filter.QConfigVersionQueryParam;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataResponse;
import com.ctrip.framework.drc.console.vo.filter.QConfigVersionResponse;
import com.ctrip.framework.drc.console.vo.filter.UpdateQConfigResponse;

/**
 * Created by dengquanliang
 * 2023/4/24 14:35
 */
public interface QConfigApiService {

    QConfigDataResponse getQConfigData(QConfigQueryParam param);

    QConfigVersionResponse getQConfigVersion(QConfigVersionQueryParam param);

    /**
     * batchUpdate single config
     * @param param
     * @return
     */
    UpdateQConfigResponse batchUpdateConfig(QConfigBatchUpdateParam param);
}
