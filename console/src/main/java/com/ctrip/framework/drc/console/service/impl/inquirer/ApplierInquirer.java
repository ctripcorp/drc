package com.ctrip.framework.drc.console.service.impl.inquirer;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;

import java.util.List;

/**
 * Created by shiruixin
 * 2024/9/11 17:55
 */
public class ApplierInquirer extends AbstractInquirer<ApplierInfoDto> {
    @Override
    String method() {
        return "appliers/info/all";
    }

    @Override
    List<ApplierInfoDto> parseData(ApiResult<?> data) {
        return JsonUtils.fromJsonToList(JsonUtils.toJson(data.getData()), ApplierInfoDto.class);
    }

    private static class ApplierInquirerHolder {
        private static final ApplierInquirer INSTANCE = new ApplierInquirer();
    }

    public static ApplierInquirer getInstance() {
        return ApplierInquirer.ApplierInquirerHolder.INSTANCE;
    }
}
