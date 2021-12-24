package com.ctrip.framework.drc.core.service.beacon;


/**
 * @ClassName BlankBeaconApiServiceImpl
 * @Author haodongPan
 * @Date 2021/12/6 16:40
 * @Version: $
 */
public class BlankBeaconApiServiceImpl implements BeaconApiService {

    @Override
    public BeaconResult doRegister(String beaconPrefix, String systemName, String dalCluster, RegisterDto registerDto) {
        return null;
    }

    @Override
    public BeaconResult getRegisteredClusters(String beaconPrefix, String systemName) {
        return null;
    }

    @Override
    public BeaconResult deRegister(String beaconPrefix, String systemName, String dalCluster) {
        return null;
    }

    @Override
    public int getOrder() {
        return 1;
    }
}
