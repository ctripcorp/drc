package com.ctrip.framework.drc.validation.activity.monitor;

import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.server.config.validation.dto.ValidationResultDto;
import com.ctrip.framework.drc.fetcher.activity.monitor.ReportActivity;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;

import java.util.List;

public class ValidationResultActivity extends ReportActivity<ValidationResultDto, Boolean> {

    @InstanceConfig(path = "validation.result.upload.url")
    public String validationResultUploadUrl = "unset";

    @InstanceConfig(path = "validation.result.upload.switch")
    public String validationResultUploadSwitch = "unset";

    @Override
    public void doReport(List<ValidationResultDto> taskList) {

        for(ValidationResultDto dto : taskList) {
            logger.info("report url: {}, dto: {}", validationResultUploadUrl, dto);
            HttpUtils.put(validationResultUploadUrl, dto);
        }
    }

    public boolean report(ValidationResultDto resultDto) {
        if("on".equalsIgnoreCase(validationResultUploadSwitch)) {
            return trySubmit(resultDto);
        }
        return true;
    }
}