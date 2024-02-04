package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.ApplicationApprovalTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplicationFormTbl;
import com.ctrip.framework.drc.console.dao.v2.ApplicationApprovalTblDao;
import com.ctrip.framework.drc.console.dao.v2.ApplicationFormTblDao;
import com.ctrip.framework.drc.console.enums.ApprovalResultEnum;
import com.ctrip.framework.drc.console.param.v2.application.ApplicationFormBuildParam;
import com.ctrip.framework.drc.console.param.v2.application.ApplicationFormQueryParam;
import com.ctrip.framework.drc.console.service.v2.DrcApplicationService;
import com.ctrip.framework.drc.console.utils.DateUtils;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.console.vo.v2.ApplicationFormView;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2024/1/31 16:52
 */
@Service
public class DrcApplicationServiceImpl implements DrcApplicationService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ApplicationFormTblDao applicationFormTblDao;
    @Autowired
    private ApplicationApprovalTblDao applicationApprovalTblDao;

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void createApplicationForm(ApplicationFormBuildParam param) throws SQLException {
        checkApplicationFormBuildParam(param);
        ApplicationFormTbl formTbl = new ApplicationFormTbl();
        BeanUtils.copyProperties(param, formTbl);
        Long applicationFormId = applicationFormTblDao.insertWithReturnId(formTbl);

        ApplicationApprovalTbl approvalTbl = new ApplicationApprovalTbl();
        approvalTbl.setApplicationFormId(applicationFormId);
        approvalTbl.setApprovalResult(ApprovalResultEnum.NOT_APPROVED.getCode());
        approvalTbl.setApplicant(param.getApplicant());
        applicationApprovalTblDao.insert(approvalTbl);
    }

    @Override
    public List<ApplicationFormView> getApplicationForms(ApplicationFormQueryParam param) throws SQLException {
        List<ApplicationFormTbl> applicationForms = applicationFormTblDao.queryByParam(param);
        if (CollectionUtils.isEmpty(applicationForms)) {
            return new ArrayList<>();
        }

        List<Long> applicationFormIds = applicationForms.stream().map(ApplicationFormTbl::getId).collect(Collectors.toList());
        List<ApplicationApprovalTbl> applicationApprovals = applicationApprovalTblDao.queryByApplicationFormIds(applicationFormIds);
        Map<Long, ApplicationApprovalTbl> applicationApprovalMap = applicationApprovals.stream().collect(Collectors.toMap(ApplicationApprovalTbl::getApplicationFormId, Function.identity()));
        List<ApplicationFormView> views = applicationForms.stream().map(source -> {
            ApplicationFormView target = new ApplicationFormView();
            BeanUtils.copyProperties(source, target, "createTime");
            target.setApplicationFormId(source.getId());
            target.setCreateTime(DateUtils.longToString(source.getCreateTime().getTime()));
            target.setUpdateTime(DateUtils.longToString(source.getDatachangeLasttime().getTime()));

            ApplicationApprovalTbl approvalTbl = applicationApprovalMap.get(source.getId());
            target.setApplicant(approvalTbl.getApplicant());
            target.setApprovalResult(approvalTbl.getApprovalResult());
            target.setOperator(approvalTbl.getOperator());
            return target;
        }).collect(Collectors.toList());
        return views;
    }

    private void checkApplicationFormBuildParam(ApplicationFormBuildParam param) {
        PreconditionUtils.checkString(param.getBuName(), "BU is empty");
        PreconditionUtils.checkString(param.getSrcRegion(), "srcRegion is empty");
        PreconditionUtils.checkString(param.getDstRegion(), "dstRegion is empty");
        PreconditionUtils.checkArgument(!param.getSrcRegion().equals(param.getDstRegion()), "srcRegion and dstRegion cannot be the same");
        PreconditionUtils.checkNotNull(param.getOrderRelated(), "orderRelated is empty");
        PreconditionUtils.checkString(param.getFilterType(), "filterType is empty");
        PreconditionUtils.checkString(param.getTag(), "tag is empty");
        PreconditionUtils.checkString(param.getApplicant(), "applicant is empty");
        PreconditionUtils.checkNotNull(param.getFlushExistingData(), "flushExistingData is empty");
    }
}
