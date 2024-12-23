package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.core.entity.Instance;

import java.util.List;
import java.util.Map;

/**
 * @author yongnian
 * @create 2024/12/17 13:36
 */
public interface RemoteResourceService {
    Map<String, List<Instance>> getCurrentDbApplierInstances(String srcMha, String mhaName, List<String> ips);

    List<Instance> getCurrentReplicatorInstance(String mhaName, List<String> ip);

    List<Instance> getCurrentMessengerInstance(String mhaName, List<String> ip);
}
