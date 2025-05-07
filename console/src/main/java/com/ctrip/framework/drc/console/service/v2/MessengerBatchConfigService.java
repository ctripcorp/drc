package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dto.v3.DbMqConfigInfoDto;
import com.ctrip.framework.drc.console.dto.v3.DbMqCreateDto;
import com.ctrip.framework.drc.console.dto.v3.DbMqEditDto;

public interface MessengerBatchConfigService {


    void processCreateMqConfig(DbMqCreateDto createDto, DbMqConfigInfoDto currentConfig);

    void processDeleteMqConfig(DbMqEditDto editDto, DbMqConfigInfoDto dbMqConfig);

    void processUpdateMqConfig(DbMqEditDto editDto, DbMqConfigInfoDto dbMqConfig);

    void refreshRegistryConfig(DbMqConfigInfoDto currentConfig);
}
