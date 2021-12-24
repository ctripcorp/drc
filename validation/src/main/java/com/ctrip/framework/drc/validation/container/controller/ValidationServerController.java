package com.ctrip.framework.drc.validation.container.controller;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.validation.dto.ValidationConfigDto;
import com.ctrip.framework.drc.validation.container.ValidationServerContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/validations")
public class ValidationServerController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ValidationServerContainer serverContainer;

    /**
     curl -H "Content-Type:application/json" -X PUT -d '{
         "gtidExecuted":"266e5755-124e-11ea-9b7b-98039bbedf9c:366561-370331,560f4cad-8c39-11e9-b53b-6c92bf463216:1-5476947662",
         "replicator": {
             "mhaName": "fat-fx-drc1",
             "port": 8383,
             "ip" : "127.0.0.1",
             "cluster": "bbzdrcbenchmarkdb_dalcluster"
         },
         "machines": [
        ]
     }' 'http://127.0.0.1:8080/validations'
     */
    @RequestMapping(method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult put(@RequestBody ValidationConfigDto config) {
        logger.info("[Add] put validation: {}", config);
        return doAddServer(config);
    }

    /**
     curl -H "Content-Type:application/json" -X DELETE 'http://127.0.0.1:8080/validations/registryKeys/testcluster.testmha'
     */
    @RequestMapping(value = "/registryKeys/{registryKey:.+}", method = RequestMethod.DELETE)
    public ApiResult remove(@PathVariable String registryKey) {
        logger.info("[Remove] validation: {}", registryKey);
        return doRemoveServer(registryKey);
    }

    private ApiResult doRemoveServer(String registryKey) {
        try {
            serverContainer.removeServer(registryKey);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("[Remove] validation: {} error", registryKey, e);
            return ApiResult.getFailInstance(false);
        }
    }

    private ApiResult doAddServer(ValidationConfigDto config) {
        try {
            serverContainer.addServer(config);
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        } catch (Exception e){
            logger.error("doAddServer error for {}", config.getRegistryKey(), e);
            return ApiResult.getFailInstance(false);
        }
    }
}
