package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.dto.FailoverDto;
import com.ctrip.framework.drc.core.service.beacon.BeaconResult;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.HealthServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

import static com.ctrip.framework.drc.core.service.beacon.BeaconResult.BEACON_FAILURE;
import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-25
 */
@RestController
@RequestMapping("/api/drc/v1/beacon/")
public class HealthController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    protected static final String BEACON_FAILOVER_TOKEN_VALUE = "beacon-failover";

    protected static final String BEACON_FAILOVER_TOKEN_KEY = "token";

    @Autowired
    private HealthServiceImpl healthService;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    /**
     * @param clusterName: [Beacon Definition] dal cluster name for DRC case
     * @param groupName: [Beacon Definition] mha name for DRC case
     * @return
     */
    @GetMapping("health")
    public BeaconResult queryClusterHealth(@RequestParam String clusterName, @RequestParam String groupName) {
        logger.info("[Beacon] query health {}({})", clusterName, groupName);
        Boolean delayNormal = healthService.isDelayNormal(clusterName, groupName);
        logger.info("[Beacon] query health result {}({}):{}", clusterName, groupName, delayNormal);
        Map<String, Object> map = new HashMap<>();
        map.put("result", delayNormal);
        return delayNormal ? BeaconResult.getSuccessInstance(map) : BeaconResult.getSuccessInstance(map, BEACON_FAILURE);
    }

    @PostMapping("failover")
    public BeaconResult doFailover(@RequestBody  FailoverDto failoverDto) {
        logger.info("[Beacon] do failover: {}", failoverDto);
        if(SWITCH_STATUS_ON.equalsIgnoreCase(monitorTableSourceProvider.getAllowFailoverSwitch())) {
            Map extra = failoverDto.getExtra();
            if (extra == null || !BEACON_FAILOVER_TOKEN_VALUE.equalsIgnoreCase(String.valueOf(extra.get(BEACON_FAILOVER_TOKEN_KEY)))) {
                return BeaconResult.getFailInstance(false, "beacon failover token is incorrect or unregistered");
            }
            return healthService.doFailover(failoverDto);
        }
        logger.info("[Beacon] do failover switch is not on, open it through qconfig");
        return BeaconResult.getSuccessInstance(true);
    }

    @GetMapping("function/mhas/{mha}")
    public BeaconResult queryUpdateFunction(@PathVariable String mha) {
        logger.info("[Beacon] check update health {}", mha);
        Boolean updateFunction = healthService.isLocalDcUpdating(mha);
        logger.info("[Beacon] check update health {} result {}", mha, updateFunction);
        return updateFunction ? BeaconResult.getSuccessInstance(true) : BeaconResult.getFailInstance(false);
    }
}
