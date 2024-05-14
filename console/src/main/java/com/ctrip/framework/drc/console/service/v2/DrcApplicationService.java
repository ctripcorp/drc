package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.param.v2.application.ApplicationFormBuildParam;
import com.ctrip.framework.drc.console.param.v2.application.ApplicationFormQueryParam;
import com.ctrip.framework.drc.console.vo.v2.ApplicationFormView;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2024/1/31 16:42
 */
public interface DrcApplicationService {

    void createApplicationForm(ApplicationFormBuildParam param) throws SQLException;

    void deleteApplicationForm(long applicationFormId) throws Exception;

    List<ApplicationFormView> getApplicationForms(ApplicationFormQueryParam param) throws SQLException;

    void approveForm(long applicationFormId) throws Exception;

    boolean sendEmail(long applicationFormId) throws Exception;

    boolean updateApplicant(long applicationFormId, String applicant) throws Exception;
}
