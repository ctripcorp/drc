package com.ctrip.framework.drc.validation.server;

import com.ctrip.framework.drc.core.server.config.validation.dto.ValidationConfigDto;
import com.ctrip.framework.drc.validation.AllTests;

/**
 * @author: Haibo Shen
 * @Date: 2021/3/26
 */
public class LocalValidationServer extends ValidationServer {

    public LocalValidationServer() throws Exception {
        ValidationConfigDto configDto = AllTests.getValidationConfigDto();
        setConfig(configDto, ValidationConfigDto.class);
        setName(configDto.getRegistryKey());
        define();
    }
}
