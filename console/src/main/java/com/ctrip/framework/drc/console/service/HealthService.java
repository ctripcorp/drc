package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dto.FailoverDto;
import com.ctrip.framework.drc.core.service.beacon.RegisterDto;
import com.ctrip.framework.drc.core.service.beacon.BeaconResult;

import java.util.List;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-25
 */
public interface HealthService {
    Boolean isDelayNormal(String dalClusterName, String mhaName);

    BeaconResult doFailover(FailoverDto failoverDto);

    Boolean isLocalDcUpdating(String mha);

    Boolean isTargetDcUpdating(String mha);

    BeaconResult doRegister(String dalCluster, RegisterDto registerDto, String systemName);

    List<String> deRegister(List<String> dalClusters, String systemName);

    List<String> getRegisteredClusters(String systemName);
}
