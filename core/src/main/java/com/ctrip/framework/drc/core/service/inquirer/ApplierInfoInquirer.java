package com.ctrip.framework.drc.core.service.inquirer;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;

import java.util.List;

/**
 * @author yongnian
 * @create: 2024/5/20 20:58
 */
public class ApplierInfoInquirer extends AbstractInfoInquirer<ApplierInfoDto> {

    private static class ApplierInfoInquirerHolder {
        private static final ApplierInfoInquirer INSTANCE = new ApplierInfoInquirer();
    }

    @Override
    String method() {
        return "appliers/info/all";
    }

    public static ApplierInfoInquirer getInstance() {
        return ApplierInfoInquirer.ApplierInfoInquirerHolder.INSTANCE;
    }


    @Override
    List<ApplierInfoDto> parseData(ApiResult<?> apiResult) {
        return JsonUtils.fromJsonToList(JsonUtils.toJson(apiResult.getData()), ApplierInfoDto.class);
    }
}
