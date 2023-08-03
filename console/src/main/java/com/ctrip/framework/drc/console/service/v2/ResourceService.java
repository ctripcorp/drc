package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.param.v2.ResourceBuildParam;
import com.ctrip.framework.drc.console.param.v2.ResourceQueryParam;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.core.http.PageResult;

/**
 * Created by dengquanliang
 * 2023/8/3 16:10
 */
public interface ResourceService {

    void configureResource(ResourceBuildParam param) throws Exception;

    void offlineResource(long resourceId) throws Exception;

    void onlineResource(long resourceId) throws Exception;

    void deactivateResource(long resourceId) throws Exception;

    void recoverResource(long resourceId) throws Exception;

    PageResult<ResourceView> getResourceView(ResourceQueryParam param);
}
