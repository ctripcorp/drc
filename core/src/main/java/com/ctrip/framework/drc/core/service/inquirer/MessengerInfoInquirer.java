package com.ctrip.framework.drc.core.service.inquirer;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerInfoDto;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;

import java.util.List;

/**
 * Created by shiruixin
 * 2024/12/3 16:15
 */
public class MessengerInfoInquirer extends AbstractInfoInquirer<MessengerInfoDto> {
    @Override
    String method() {
        return "messengers/info/all";
    }

    @Override
    List<MessengerInfoDto> parseData(ApiResult<?> data) {
        return JsonUtils.fromJsonToList(JsonUtils.toJson(data.getData()), MessengerInfoDto.class);
    }

    private static class MessengerInquirerHolder {
        private static final MessengerInfoInquirer INSTANCE = new MessengerInfoInquirer();
    }

    public static MessengerInfoInquirer getInstance() {
        return MessengerInfoInquirer.MessengerInquirerHolder.INSTANCE;
    }
}
