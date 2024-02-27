package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplicationApprovalTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplicationFormTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplicationRelationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ReplicationTableTbl;
import com.ctrip.framework.drc.console.dao.v2.ApplicationApprovalTblDao;
import com.ctrip.framework.drc.console.dao.v2.ApplicationFormTblDao;
import com.ctrip.framework.drc.console.dao.v2.ApplicationRelationTblDao;
import com.ctrip.framework.drc.console.dao.v2.ReplicationTableTblDao;
import com.ctrip.framework.drc.console.dto.v2.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v2.MhaDto;
import com.ctrip.framework.drc.console.dto.v2.MhaReplicationDto;
import com.ctrip.framework.drc.console.enums.ApprovalResultEnum;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.v2.EffectiveStatusEnum;
import com.ctrip.framework.drc.console.param.v2.application.ApplicationFormBuildParam;
import com.ctrip.framework.drc.console.param.v2.application.ApplicationFormQueryParam;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.v2.DrcApplicationService;
import com.ctrip.framework.drc.console.service.v2.MhaReplicationServiceV2;
import com.ctrip.framework.drc.console.utils.DateUtils;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.console.vo.v2.ApplicationFormView;
import com.ctrip.framework.drc.core.service.email.Email;
import com.ctrip.framework.drc.core.service.email.EmailResponse;
import com.ctrip.framework.drc.core.service.email.EmailService;
import com.ctrip.framework.drc.core.service.user.UserService;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.base.Joiner;
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
    @Autowired
    private ApplicationRelationTblDao applicationRelationTblDao;
    @Autowired
    private ReplicationTableTblDao replicationTableTblDao;
    @Autowired
    private MhaReplicationServiceV2 mhaReplicationServiceV2;
    @Autowired
    private DomainConfig domainConfig;

    private EmailService emailService = ApiContainer.getEmailServiceImpl();
    private UserService userService = ApiContainer.getUserServiceImpl();

    private static final long MAX_DELAY = 10000;

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

    @Override
    public void approveForm(long applicationFormId) throws Exception {
        ApplicationApprovalTbl approvalTbl = applicationApprovalTblDao.queryByApplicationFormId(applicationFormId);
        approvalTbl.setApprovalResult(ApprovalResultEnum.UNDER_APPROVAL.getCode());

        String username = userService.getInfo();
        approvalTbl.setOperator(username);
        applicationApprovalTblDao.update(approvalTbl);
    }

    @Override
    public boolean sendEmail(long applicationFormId) throws Exception {
        ApplicationFormTbl applicationForm = applicationFormTblDao.queryById(applicationFormId);
        if (applicationForm.getIsSentEmail().equals(BooleanEnum.TRUE.getCode())) {
            logger.info("applicationFormId: {} has already sent email", applicationFormId);
            return false;
        }
        ApplicationApprovalTbl approvalTbl = applicationApprovalTblDao.queryByApplicationFormId(applicationFormId);
        List<ApplicationRelationTbl> applicationRelationTbls = applicationRelationTblDao.queryByApplicationFormId(applicationFormId);
        List<Long> dbReplicationIds = applicationRelationTbls.stream().map(ApplicationRelationTbl::getDbReplicationId).collect(Collectors.toList());
        List<ReplicationTableTbl> replicationTableTbls = replicationTableTblDao.queryByDbReplicationIds(dbReplicationIds, EffectiveStatusEnum.IN_EFFECT.getCode());
        if (CollectionUtils.isEmpty(replicationTableTbls)) {
            logger.info("applicationFormId: {} replicationTableTbls not in effect", applicationFormId);
            return false;
        }

        List<MhaReplicationDto> mhaReplicationDtos = replicationTableTbls.stream().map(source -> {
            MhaReplicationDto target = new MhaReplicationDto();
            MhaDto srcMha = new MhaDto();
            MhaDto dstMha = new MhaDto();
            srcMha.setName(source.getSrcMha());
            srcMha.setRegionName(source.getSrcRegion());
            dstMha.setName(source.getDstMha());
            dstMha.setRegionName(source.getDstRegion());

            target.setSrcMha(srcMha);
            target.setDstMha(dstMha);
            return target;
        }).distinct().collect(Collectors.toList());
        boolean delayCorrect = checkMhaReplicationDelays(mhaReplicationDtos);
        if (!delayCorrect) {
            logger.info("applicationFormId: {} delay not ready", applicationFormId);
            return false;
        }

        List<String> dbNames = replicationTableTbls.stream().map(ReplicationTableTbl::getDbName).distinct().collect(Collectors.toList());
        List<ReplicationTableTbl> newAddTables = replicationTableTbls.stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ReplicationTableTbl> deletedTables = replicationTableTbls.stream().filter(e -> e.getDeleted().equals(BooleanEnum.TRUE.getCode())).collect(Collectors.toList());

        boolean result = sendEmail(applicationForm, approvalTbl, mhaReplicationDtos, dbNames, newAddTables, deletedTables);

        applicationForm.setIsSentEmail(BooleanEnum.TRUE.getCode());
        applicationFormTblDao.update(applicationForm);

        replicationTableTbls.forEach(e -> e.setEffectiveStatus(EffectiveStatusEnum.EFFECTIVE.getCode()));
        replicationTableTblDao.update(replicationTableTbls);

        return result;
    }

    private boolean sendEmail(ApplicationFormTbl applicationForm, ApplicationApprovalTbl approvalTbl, List<MhaReplicationDto> mhaReplicationDtos, List<String> dbNames, List<ReplicationTableTbl> newAddTables, List<ReplicationTableTbl> deletedTables) {
        Email email = new Email();
        email.setSubject("DRC同步配置变更");
        email.setSender(domainConfig.getDrcConfigSenderEmail());
        if (domainConfig.getDrcConfigEmailSendSwitch()) {
            email.addRecipient(approvalTbl.getApplicant() + "@trip.com");
            domainConfig.getDrcConfigCcEmail().forEach(email::addCc);
            email.addCc(domainConfig.getDrcConfigDbaEmail());
        } else {
            domainConfig.getDrcConfigCcEmail().forEach(email::addRecipient);
        }
        if (applicationForm.getFlushExistingData().equals(BooleanEnum.TRUE.getCode())) {
            email.setHeader("DRC同步已配置，请DBA处理存量数据!");
        } else {
            email.setHeader("DRC同步已配置，请业务验证!");
        }
        email.addContentKeyValue("延迟监控", buildMhaDelayUrl(mhaReplicationDtos));
        email.addContentKeyValue("同步DB", Joiner.on(",").join(dbNames));
        if (!CollectionUtils.isEmpty(newAddTables)) {
            List<String> tableNames = newAddTables.stream().map(ReplicationTableTbl::getTableName).collect(Collectors.toList());
            email.addContentKeyValue("新增表", Joiner.on(",").join(tableNames) + "共" + tableNames.size() + "张表");
        }
        if (!CollectionUtils.isEmpty(deletedTables)) {
            List<String> tableNames = deletedTables.stream().map(ReplicationTableTbl::getTableName).collect(Collectors.toList());
            email.addContentKeyValue("删除表", Joiner.on(",").join(tableNames) + "共" + tableNames.size() + "张表");
        }
        EmailResponse emailResponse = emailService.sendEmail(email);
        if (emailResponse.isSuccess()) {
            logger.info("[[task=drcConfigSendEmail]] send email success, applicationFormId: {}", applicationForm);
            return true;
        } else {
            logger.error("[[task=drcConfigSendEmailFail]] send email fail, applicationFormId: {}", applicationForm);
            return false;
        }
    }

    private String buildMhaDelayUrl(List<MhaReplicationDto> mhaReplicationDtos) {
        StringBuilder stringBuilder = new StringBuilder();
        int size = mhaReplicationDtos.size();
        for (int i = 0; i < size; i++) {
            MhaReplicationDto mhaReplicationDto = mhaReplicationDtos.get(i);
            MhaDto srcMha = mhaReplicationDto.getSrcMha();
            MhaDto dstMha = mhaReplicationDto.getDstMha();
            stringBuilder.append("<a href='").append(domainConfig.getConflictAlarmHickwallUrl() + "&var-mha=" + srcMha.getName() + "'>")
                    .append(srcMha.getName() + "(" + srcMha.getRegionName() + ")" + "=>" + dstMha.getName() + "(" + dstMha.getRegionName() + ")").append("</a>");
            if (i != size - 1) {
                stringBuilder.append("</br>");
            }
        }
        return stringBuilder.toString();
    }

    private boolean checkMhaReplicationDelays(List<MhaReplicationDto> mhaReplicationDtoList) {
        List<MhaDelayInfoDto> mhaReplicationDelays;
        try {
            mhaReplicationDelays = mhaReplicationServiceV2.getMhaReplicationDelays(mhaReplicationDtoList);
        } catch (Exception e) {
            return false;
        }
        if (CollectionUtils.isEmpty(mhaReplicationDelays) || mhaReplicationDelays.size() != mhaReplicationDtoList.size()) {
            return false;
        }

        for (MhaDelayInfoDto mhaDelayInfoDto : mhaReplicationDelays) {
            if (mhaDelayInfoDto.getDelay() > MAX_DELAY) {
                return false;
            }
        }
        return true;
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
