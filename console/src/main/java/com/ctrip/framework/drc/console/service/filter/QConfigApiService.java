package com.ctrip.framework.drc.console.service.filter;

import com.ctrip.framework.drc.console.param.filter.QConfigBatchUpdateParam;
import com.ctrip.framework.drc.console.param.filter.QConfigQueryParam;
import com.ctrip.framework.drc.console.param.filter.QConfigRevertParam;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataResponse;
import com.ctrip.framework.drc.console.vo.filter.UpdateQConfigResponse;

/**
 * Created by dengquanliang
 * 2023/4/24 14:35
 */
public interface QConfigApiService {

    QConfigDataResponse getQConfigData(QConfigQueryParam param);

    /**
     * batchUpdate single config
     * @param param
     * @return
     */
    UpdateQConfigResponse batchUpdateConfig(QConfigBatchUpdateParam param);

    /**
     * revert config to last version
     * @param param
     * @return
     */
    UpdateQConfigResponse revertConfig(QConfigRevertParam param);
}
