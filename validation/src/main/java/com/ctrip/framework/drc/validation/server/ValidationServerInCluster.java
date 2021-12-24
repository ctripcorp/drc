package com.ctrip.framework.drc.validation.server;

import com.ctrip.framework.drc.core.server.config.validation.dto.ValidationConfigDto;

/**
 * @author: Haibo Shen
 * @Date: 2021/3/16
 */
public class ValidationServerInCluster extends ValidationServer {

    public ValidationConfigDto config;

    public ValidationServerInCluster(ValidationConfigDto config) throws Exception {
        setConfig(config, ValidationConfigDto.class);
        setName(config.getRegistryKey());
        define();
        this.config = config;
    }
}
