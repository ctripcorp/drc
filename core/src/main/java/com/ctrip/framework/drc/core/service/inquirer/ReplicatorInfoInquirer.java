package com.ctrip.framework.drc.core.service.inquirer;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
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

    @Override
    public String name() {
        return ConsumeType.Replicator.name().toLowerCase();
    }

    public static ReplicatorInfoInquirer getInstance() {
        return ReplicatorInfoInquirer.ReplicatorInfoInquirerHolder.INSTANCE;
    }


    @Override
    List<ReplicatorInfoDto> parseData(ApiResult<?> apiResult) {
        return JsonUtils.fromJsonToList(JsonUtils.toJson(apiResult.getData()), ReplicatorInfoDto.class);
    }
}
