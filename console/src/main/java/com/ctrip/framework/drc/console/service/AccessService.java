package com.ctrip.framework.drc.console.service;

import java.util.Map;


/**
 * @author maojiawei
 * @version 1.0
 * date: 2020-07-28
 */
public interface AccessService {

    Map<String, Object> applyPreCheck(String requestBody);

    Map<String, Object> deployDns(String requestBody);

    Map<String, Object> buildMhaCluster(String requestBody);

    Map<String, Object> buildMhaClusterV2(String requestBody) throws Exception;

    Map<String, Object> getCopyResult(String requestBody);

    Map<String, Object> registerDalCluster(String requestBody, String env, String goal);

    Map<String, Object> releaseDalCluster(String dalClusterName, String env);
}
