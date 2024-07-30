package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.aop.permission.AccessToken;
import com.ctrip.framework.drc.console.enums.TokenType;
import com.ctrip.framework.drc.console.param.v2.application.ApplicationFormBuildParam;
import com.ctrip.framework.drc.console.param.v2.application.ApplicationFormQueryParam;
import com.ctrip.framework.drc.console.service.v2.DrcApplicationService;
import com.ctrip.framework.drc.console.vo.v2.ApplicationFormView;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Created by dengquanliang
 * 2024/2/4 15:20
 */
@RestController
@RequestMapping("/api/drc/v2/application/")
public class DrcApplicationController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DrcApplicationService drcApplicationService;

    @GetMapping("list")
    public ApiResult<List<ApplicationFormView>> getApplicationForms(ApplicationFormQueryParam param) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(drcApplicationService.getApplicationForms(param));
            apiResult.setPageReq(param.getPageReq());
            return apiResult;
        } catch (Exception e) {
            logger.error("getApplicationForms fail, ", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @DeleteMapping()
    public ApiResult<Boolean> deleteApplicationForm(@RequestParam long applicationFormId) {
        try {
            drcApplicationService.deleteApplicationForm(applicationFormId);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("deleteApplicationForm fail, ", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("")
    public ApiResult<Boolean> createApplicationForm(@RequestBody ApplicationFormBuildParam param) {
        try {
            drcApplicationService.createApplicationForm(param);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("createApplicationForm fail, ", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @AccessToken(type = TokenType.OPEN_API_4_DBA)
    @PostMapping("dba")
    public ApiResult<Boolean> createApplicationFormForDba(@RequestBody ApplicationFormBuildParam param) {
        try {
            drcApplicationService.createApplicationForm(param);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("createApplicationForm fail, ", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("email")
    public ApiResult<Boolean> sendEmail(@RequestParam Long applicationFormId) {
        try {
            return ApiResult.getSuccessInstance(drcApplicationService.manualSendEmail(applicationFormId));
        } catch (Exception e) {
            logger.error("createApplicationForm fail, ", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("approve")
    public ApiResult<Boolean> approveForm(@RequestParam Long applicationFormId) {
        try {
            drcApplicationService.approveForm(applicationFormId);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("createApplicationForm fail, ", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("applicant")
    public ApiResult<Boolean> updateApplicant(@RequestParam Long applicationFormId, @RequestParam String applicant) {
        try {
            drcApplicationService.updateApplicant(applicationFormId, applicant);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("updateApplicant fail, ", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

}
