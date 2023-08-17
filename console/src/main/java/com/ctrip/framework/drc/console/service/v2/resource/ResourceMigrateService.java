package com.ctrip.framework.drc.console.service.v2.resource;

import com.ctrip.framework.drc.console.param.v2.resource.DeleteIpParam;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceBuildParam;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/8 10:45
 */
public interface ResourceMigrateService {

    List<ResourceView> getResourceUnused(int type) throws Exception;

    List<String> getDeletedIps(DeleteIpParam param);

    int deleteResourceUnused(List<String> ips) throws Exception;

    int updateResource(List<ResourceBuildParam> params) throws Exception;

    int updateResource(String dc) throws Exception;

    int updateMhaTag() throws Exception;

    List<Long> getReplicatorGroupIdsWithSameAz() throws Exception;

    List<Long> getApplierGroupIdsWithSameAz() throws Exception;

    List<Long> getMessengerGroupIdsWithSameAz() throws Exception;
}
