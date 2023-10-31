package com.ctrip.framework.drc.service.console.service;

import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.beacon.BeaconApiService;
import com.ctrip.framework.drc.core.service.beacon.BeaconResult;
import com.ctrip.framework.drc.core.service.beacon.RegisterDto;

/**
 * @ClassName BeaconApiServiceImpl
 * @Author haodongPan
 * @Date 2021/11/24 20:30
 * @Version: $
 */
public class BeaconApiServiceImpl implements BeaconApiService {
    
    public static final String BEACON_REGISTER = "monitor/%s/cluster/%s";

    public static final String BEACON_CLUSTER_QUERY = "monitor/%s/clusters";

    public static final String BEACON_DEREGISTER = "monitor/%s/cluster/%s";
    
    
    @Override
    public BeaconResult doRegister(String beaconPrefix, String systemName, String dalCluster, RegisterDto registerDto) {
        String uri = String.format(beaconPrefix + BEACON_REGISTER, systemName, dalCluster);
        return HttpUtils.post(uri, registerDto, BeaconResult.class);
    }

    @Override
    public BeaconResult getRegisteredClusters(String beaconPrefix, String systemName) {
        String uri = String.format(beaconPrefix + BEACON_CLUSTER_QUERY, systemName);
        return HttpUtils.get(uri, BeaconResult.class);
    }

    @Override
    public BeaconResult deRegister(String beaconPrefix, String systemName, String dalCluster) {
        String uri = String.format(beaconPrefix + BEACON_DEREGISTER, systemName, dalCluster);
        return HttpUtils.delete(uri, BeaconResult.class);
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
