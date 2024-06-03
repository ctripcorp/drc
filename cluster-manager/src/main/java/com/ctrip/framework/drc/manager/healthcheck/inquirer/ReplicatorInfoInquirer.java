package com.ctrip.framework.drc.manager.healthcheck.inquirer;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorInfoDto;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;

import java.util.List;

/**
 * @author yongnian
 * @create: 2024/5/20 20:58
 */
public class ReplicatorInfoInquirer extends AbstractInfoInquirer<ReplicatorInfoDto> {

    private static class ReplicatorInfoInquirerHolder {
        private static final ReplicatorInfoInquirer INSTANCE = new ReplicatorInfoInquirer();
    }

    @Override
    String method() {
        return "replicators/info/all";
    }

    public static ReplicatorInfoInquirer getInstance() {
        return ReplicatorInfoInquirer.ReplicatorInfoInquirerHolder.INSTANCE;
    }


    @Override
    List<ReplicatorInfoDto> parseData(ApiResult<?> apiResult) {
        return JsonUtils.fromJsonToList(JsonUtils.toJson(apiResult.getData()), ReplicatorInfoDto.class);
    }
}
