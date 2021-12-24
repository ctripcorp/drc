package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dto.FailoverDto;
import com.ctrip.framework.drc.core.service.beacon.RegisterDto;
import com.ctrip.framework.drc.core.service.dal.DalClusterTypeEnum;
import com.ctrip.framework.drc.core.service.enums.EnvEnum;
import com.ctrip.framework.drc.core.service.beacon.BeaconResult;
import com.ctrip.framework.drc.console.monitor.cases.function.DatachangeLastTimeMonitorCase;
import com.ctrip.framework.drc.console.monitor.delay.DelayMap;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.core.service.beacon.BeaconApiService;
import com.ctrip.framework.drc.console.service.HealthService;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.foundation.Env;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.ctrip.framework.drc.core.service.beacon.BeaconResult.BEACON_SUCCESS;
import static com.ctrip.framework.drc.console.service.impl.DalServiceImpl.DAL_SUCCESS_CODE;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-25
 */
@Service
public class HealthServiceImpl implements HealthService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DbClusterSourceProvider sourceProvider;

    @Autowired
    private DalServiceImpl dalService;

    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;
    
    private BeaconApiService beaconApiServiceImpl = ApiContainer.getBeaconApiServiceImpl();

    protected ObjectMapper objectMapper = new ObjectMapper();

    private DelayMap delayMap = DelayMap.getInstance();

    public static final int SILENT_CAPACITY = 300;

    public static final double NORMAL_DELAY_THRESHOLD = 5000.0;

    public static final long MAX_TOLERANCE_UPDATE_TIME = 1500;

    public static final String HEALTH_IS_UPATING = "/api/drc/v1/beacon/function/mhas/%s";

    private final Env env = Foundation.server().getEnv();

    @Override
    public Boolean isDelayNormal(String dalClusterName, String mhaName) {
        List<String> targetDcMha = sourceProvider.getTargetDcMha(mhaName);
        DelayMap.DrcDirection drcDirection = new DelayMap.DrcDirection(targetDcMha.get(1), mhaName);
        double mean = delayMap.avg(drcDirection);
        int size = delayMap.size(drcDirection);
        logger.info("[Beacon] {}({}->{}) {}ms(<{}={}), delayListSize:{}(<{}={})", dalClusterName, drcDirection.getSrcMha(), drcDirection.getDestMha(), mean, NORMAL_DELAY_THRESHOLD, mean < NORMAL_DELAY_THRESHOLD, size, SILENT_CAPACITY, size < SILENT_CAPACITY);
        if(mean < NORMAL_DELAY_THRESHOLD || size < SILENT_CAPACITY) {
            return true;
        }

        Boolean targetDcUpdating = isTargetDcUpdating(mhaName);
        logger.info("[Beacon] {}({}->{}) {}ms(<{}={}), delayListSize:{}(<{}={}), targetDcUpdating={}", dalClusterName, drcDirection.getSrcMha(), drcDirection.getDestMha(), mean, NORMAL_DELAY_THRESHOLD, false, size, SILENT_CAPACITY, false, targetDcUpdating);
        return !targetDcUpdating;
    }

    @Override
    public BeaconResult doFailover(FailoverDto failoverDto) {
        String dalClusterName = failoverDto.getClusterName();
        List<FailoverDto.NodeStatusInfo> groups = failoverDto.getGroups();
        Set<String> downZones = Sets.newHashSet();
        Set<String> allZones = Sets.newHashSet();
        groups.forEach(group -> {
            String idc = group.getIdc();
            if(group.getDown()) {
                downZones.add(idc);
            }
            allZones.add(idc);
        });
        if(downZones.size() < 1 || downZones.size() == allZones.size()) {
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.beacon.failover.wrong", dalClusterName);
            logger.info("[[dalcluster={}]][Beacon] wrong failover instruction: make sure downZone size({}) is in [1, {})", dalClusterName, downZones.size(), allZones.size());
            return BeaconResult.getFailInstanceWithoutRetry(false, "wrong failover instruction, make sure downZone size( " + downZones.size() + " ) is in [1, "+ allZones.size() + "): " + failoverDto);
        }
        Set<String> upZones = Sets.newHashSet();
        upZones.addAll(allZones);
        upZones.removeAll(downZones);
        String upZone = upZones.iterator().next();
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.beacon.failover.right", dalClusterName + "." + upZone);
        logger.info("[[dalcluster={}]][Beacon] failover to {}", dalClusterName, upZone);
        try {
            ApiResult responseBody = dalService.switchDalClusterType(dalClusterName, EnvEnum.getEnvEnum(env).getEnv(), DalClusterTypeEnum.NORMAL, upZone);
            if(DAL_SUCCESS_CODE.equals(responseBody.getStatus())) {
                logger.info("[[dalcluster={}]][Beacon] failover to {} succeeded", dalClusterName, upZone);
                return BeaconResult.getSuccessInstance(true, dalClusterName + " failover to " + upZone + " succeeded. Dal response: " + responseBody.getMessage());
            } else {
                logger.info("[[dalcluster={}]][Beacon] failover to {} failed, DAL response: {}", dalClusterName, upZone, responseBody.getMessage());
                return BeaconResult.getFailInstance(false, dalClusterName + " failover to " + upZone + " failed. Dal response: " + responseBody.getMessage());
            }
        } catch (Exception e) {
            logger.error("[[dalcluster={}]][Beacon] failover to {} failed, ", dalClusterName, upZone, e);
            return BeaconResult.getFailInstance(false, dalClusterName + " failover to " + upZone + " failed. caught Dal exception: " + e);
        }
    }

    @Override
    public Boolean isLocalDcUpdating(String mha) {
        Endpoint endpoint = sourceProvider.getMasterEndpoint(mha);
        String localDcName = sourceProvider.getLocalDcName();
        WriteSqlOperatorWrapper sqlOperatorWrapper = new WriteSqlOperatorWrapper(endpoint);
        try {
            sqlOperatorWrapper.initialize();
            sqlOperatorWrapper.start();
            long delayInMillis = DatachangeLastTimeMonitorCase.getDelayInMillis(sqlOperatorWrapper, localDcName);
            logger.info("[health] local dc updating({},{},{}:{}), {}<{}?", localDcName, mha, endpoint.getHost(), endpoint.getPort(), delayInMillis, MAX_TOLERANCE_UPDATE_TIME);
            return delayInMillis < MAX_TOLERANCE_UPDATE_TIME;
        } catch (Exception e) {
            logger.warn("[health] local dc updating error({},{},{}:{}), ", localDcName, mha, endpoint.getHost(), endpoint.getPort(), e);
            return false;
        } finally {
            try {
                sqlOperatorWrapper.stop();
                sqlOperatorWrapper.dispose();
            } catch (Exception e) {
                logger.error("[[endpoint={}:{}]]sqlOperatorWrapper stop and dispose: ", endpoint.getHost(), endpoint.getPort(), e);
            }
        }
    }

    @Override
    public Boolean isTargetDcUpdating(String mha) {
        logger.info("[Beacon] check if target mha of {} updating", mha);
        List<String> targetDcMha = sourceProvider.getTargetDcMha(mha);
        if(null == targetDcMha) {
            logger.info("[Beacon] no target mha of {} found", mha);
            return false;
        }
        String targetDc = targetDcMha.get(0);
        String targetMha = targetDcMha.get(1);
        Map<String, String> consoleDcInfos = defaultConsoleConfig.getConsoleDcInfos();
        if(consoleDcInfos.containsKey(targetDc)) {
            String targetDcURI = consoleDcInfos.get(targetDc); 
            String uri = String.format(targetDcURI + HEALTH_IS_UPATING, targetMha);
            logger.info("[Beacon] querying {}", uri);
            try {
                ApiResult result = HttpUtils.get(uri);
                return (Boolean) result.getData();
            } catch (Exception e) {
                logger.warn("[Beacon] fail to query {}", uri, e);
                return false;
            }
        }
        logger.info("[Beacon] target dc {} not found ", targetDc);
        return false;
    }

    @Override
    public BeaconResult doRegister(String dalCluster, RegisterDto registerDto, String systemName) {
        logger.info("[Beacon] do register {}({}) : {}", dalCluster, systemName, registerDto);
        String beaconPrefix = defaultConsoleConfig.getBeaconPrefix();
        BeaconResult result = beaconApiServiceImpl.doRegister(beaconPrefix, systemName, dalCluster, registerDto);
        logger.info("[Beacon] register response {}({}) : {}", dalCluster, systemName, result.getMsg());
        return result;
    }

    @Override
    public List<String> deRegister(List<String> dalClusters, String systemName) {
        logger.info("[Beacon] remove registration: {}({})", dalClusters, systemName);
        List<String> deregisteredDalCluster = Lists.newArrayList();
        String beaconPrefix = defaultConsoleConfig.getBeaconPrefix();
        for(String dalCluster : dalClusters) {
            BeaconResult result = beaconApiServiceImpl.deRegister(beaconPrefix, systemName, dalCluster);
            if(result.getCode() == 0 && BEACON_SUCCESS.equalsIgnoreCase(result.getMsg())) {
                logger.info("[Beacon] remove register {}({}) succeeded", dalCluster, systemName);
                deregisteredDalCluster.add(dalCluster);
            } else {
                logger.info("[Beacon] remove register {}({}) failed", dalCluster, systemName);
            }
        }
        return deregisteredDalCluster;
    }

    @Override
    public List<String> getRegisteredClusters(String systemName) {
        logger.info("[Beacon] get all registered cluster for {}", systemName);
        String beaconPrefix = defaultConsoleConfig.getBeaconPrefix();
        try {
            BeaconResult result = beaconApiServiceImpl.getRegisteredClusters(beaconPrefix, systemName);
            if(ResultCode.HANDLE_SUCCESS.getCode() == result.getCode()) {
                logger.info("[Beacon] get all registered cluster for {} with response {}", systemName, result);
                return castList(result.getData(), String.class);
            } else {
                logger.info("[Beacon] Fail get all registered clusters for {} with response {}", systemName, result);
            }
        } catch (Exception e) {
            logger.error("[Beacon] Fail get registered cluster for {}", systemName, e);
        }
        return new ArrayList<>();
    }


    public static <T> List<T> castList(Object obj, Class<T> clazz) {
        List<T> result = new ArrayList<T>();
        if (obj instanceof List<?>) {
            for (Object o : (List<?>) obj) {
                result.add(clazz.cast(o));
            }
            return result;
        }
        return null;
    }
}
