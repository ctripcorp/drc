package com.ctrip.framework.drc.core.service.beacon;

import com.ctrip.xpipe.api.lifecycle.Ordered;

public interface BeaconApiService extends Ordered {

    BeaconResult doRegister(String beaconPrefix, String systemName, String dalCluster, RegisterDto registerDto);

    BeaconResult getRegisteredClusters(String beaconPrefix, String systemName);

    BeaconResult deRegister(String beaconPrefix, String systemName, String dalCluster);
}
