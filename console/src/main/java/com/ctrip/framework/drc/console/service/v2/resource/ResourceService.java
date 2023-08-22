package com.ctrip.framework.drc.console.service.v2.resource;

import com.ctrip.framework.drc.console.param.v2.resource.ResourceBuildParam;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceQueryParam;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceSelectParam;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;

import java.util.List;

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

    List<ResourceView> getResourceView(ResourceQueryParam param) throws Exception;

    List<ResourceView> getMhaAvailableResource(String mhaName, int type) throws Exception;

    List<ResourceView> autoConfigureResource(ResourceSelectParam param) throws Exception;
}
