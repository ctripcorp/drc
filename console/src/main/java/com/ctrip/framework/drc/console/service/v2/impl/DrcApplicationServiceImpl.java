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
import com.ctrip.framework.drc.console.dto.v3.DbApplierDto;
import com.ctrip.framework.drc.console.enums.ApprovalResultEnum;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.v2.EffectiveStatusEnum;
import com.ctrip.framework.drc.console.param.v2.application.ApplicationFormBuildParam;
import com.ctrip.framework.drc.console.param.v2.application.ApplicationFormQueryParam;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.v2.DbDrcBuildService;
import com.ctrip.framework.drc.console.service.v2.DrcApplicationService;
import com.ctrip.framework.drc.console.service.v2.MhaReplicationServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.DateUtils;
import com.ctrip.framework.drc.console.utils.MultiKey;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.console.vo.v2.ApplicationFormView;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.service.email.Email;
import com.ctrip.framework.drc.core.service.email.EmailResponse;
import com.ctrip.framework.drc.core.service.email.EmailService;
import com.ctrip.framework.drc.core.service.user.UserService;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
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
    @Autowired
    private DbDrcBuildService dbDrcBuildService;

    private EmailService emailService = ApiContainer.getEmailServiceImpl();
    private UserService userService = ApiContainer.getUserServiceImpl();

    private static final long MAX_DELAY = 10000;
    private static final String EMAIL_SUFFIX = "@trip.com";

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void createApplicationForm(ApplicationFormBuildParam param) throws SQLException {
        checkApplicationFormBuildParam(param);
        ApplicationFormTbl formTbl = new ApplicationFormTbl();
        BeanUtils.copyProperties(param, formTbl);
        formTbl.setUseGivenGtid(-1);
        Long applicationFormId = applicationFormTblDao.insertWithReturnId(formTbl);

        ApplicationApprovalTbl approvalTbl = new ApplicationApprovalTbl();
        approvalTbl.setApplicationFormId(applicationFormId);
        approvalTbl.setApprovalResult(ApprovalResultEnum.NOT_APPROVED.getCode());
        approvalTbl.setApplicant(param.getApplicant());
        applicationApprovalTblDao.insert(approvalTbl);
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void deleteApplicationForm(long applicationFormId) throws Exception {
        ApplicationFormTbl applicationFormTbl = applicationFormTblDao.queryById(applicationFormId);
        ApplicationApprovalTbl approvalTbl = applicationApprovalTblDao.queryByApplicationFormId(applicationFormId);
        if (approvalTbl.getApprovalResult() != ApprovalResultEnum.NOT_APPROVED.getCode()
                && approvalTbl.getApprovalResult() != ApprovalResultEnum.UNDER_APPROVAL.getCode()) {
            throw ConsoleExceptionUtils.message("applicationForm already approved");
        }
        applicationFormTbl.setDeleted(BooleanEnum.TRUE.getCode());
        approvalTbl.setApprovalResult(ApprovalResultEnum.CLOSED.getCode());

        applicationFormTblDao.update(applicationFormTbl);
        applicationApprovalTblDao.update(approvalTbl);
    }

    @Override
    public List<ApplicationFormView> getApplicationForms(ApplicationFormQueryParam param) throws SQLException {
        if (param.getApprovalResult() != null) {
            List<ApplicationApprovalTbl> applicationApprovalTbls = applicationApprovalTblDao.queryByApprovalResult(param.getApprovalResult());
            if (CollectionUtils.isEmpty(applicationApprovalTbls)) {
                return new ArrayList<>();
            }
            param.setApplicationFormIds(applicationApprovalTbls.stream().map(ApplicationApprovalTbl::getApplicationFormId).collect(Collectors.toList()));
        }

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
        if (approvalTbl.getApprovalResult() != ApprovalResultEnum.NOT_APPROVED.getCode()) {
            return;
        }
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
        if (!approvalTbl.getApprovalResult().equals(ApprovalResultEnum.APPROVED.getCode())) {
            logger.info("applicationFormId: {} not approved", applicationFormId);
            return false;
        }
        List<ApplicationRelationTbl> applicationRelationTbls = applicationRelationTblDao.queryByApplicationFormId(applicationFormId);
        List<Long> dbReplicationIds = applicationRelationTbls.stream().map(ApplicationRelationTbl::getDbReplicationId).collect(Collectors.toList());
        List<ReplicationTableTbl> replicationTableTbls = replicationTableTblDao.queryExistByDbReplicationIds(dbReplicationIds, EffectiveStatusEnum.IN_EFFECT.getCode());
        if (CollectionUtils.isEmpty(replicationTableTbls)) {
            logger.info("applicationFormId: {} replicationTableTbls not in effect", applicationFormId);
            return false;
        }


        Map<MultiKey, MhaReplicationDto> mhaReplicationDtoMap = replicationTableTbls.stream().collect(
                Collectors.toMap(e -> new MultiKey(e.getSrcMha(), e.getDstMha()), this::buildMhaReplicationDto, (k1, k2) ->k1));
        List<MhaReplicationDto> mhaReplicationDtos = Lists.newArrayList(mhaReplicationDtoMap.values());

        List<DbApplierDto> mhaDbAppliers = dbDrcBuildService.getMhaDbAppliers(replicationTableTbls.get(0).getSrcMha(), replicationTableTbls.get(0).getDstMha());
        boolean dbApplyMode = mhaDbAppliers.stream().anyMatch(e -> !CollectionUtils.isEmpty(e.getIps()));
        if (!dbApplyMode) {
            boolean delayCorrect = checkMhaReplicationDelays(mhaReplicationDtos);
            if (!delayCorrect) {
                logger.info("applicationFormId: {} delay not ready", applicationFormId);
                return false;
            }
        }

        List<String> dbNames = replicationTableTbls.stream().map(ReplicationTableTbl::getDbName).distinct().collect(Collectors.toList());

        boolean result = sendEmail(applicationForm, approvalTbl, mhaReplicationDtos, dbNames, replicationTableTbls);

        if (result) {
            applicationForm.setIsSentEmail(BooleanEnum.TRUE.getCode());
            applicationFormTblDao.update(applicationForm);

            replicationTableTbls.forEach(e -> e.setEffectiveStatus(EffectiveStatusEnum.EFFECTIVE.getCode()));
            replicationTableTblDao.update(replicationTableTbls);
        } else {
            logger.error("sendEmail fail, applicationFormId: {}", applicationFormId);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.CONFIG.EMAIL.FAIL", String.valueOf(applicationFormId));
        }

        return result;
    }

    @Override
    public boolean updateApplicant(long applicationFormId, String applicant) throws Exception {
        ApplicationApprovalTbl approvalTbl = applicationApprovalTblDao.queryByApplicationFormId(applicationFormId);
        approvalTbl.setApplicant(applicant);
        applicationApprovalTblDao.update(approvalTbl);
        return true;
    }

    private MhaReplicationDto buildMhaReplicationDto(ReplicationTableTbl replicationTableTbl) {
        MhaReplicationDto target = new MhaReplicationDto();
        MhaDto srcMha = new MhaDto();
        MhaDto dstMha = new MhaDto();
        srcMha.setName(replicationTableTbl.getSrcMha());
        srcMha.setRegionName(replicationTableTbl.getSrcRegion());
        dstMha.setName(replicationTableTbl.getDstMha());
        dstMha.setRegionName(replicationTableTbl.getDstRegion());

        target.setSrcMha(srcMha);
        target.setDstMha(dstMha);
        return target;
    }


    private boolean sendEmail(ApplicationFormTbl applicationForm, ApplicationApprovalTbl approvalTbl, List<MhaReplicationDto> mhaReplicationDtos, List<String> dbNames, List<ReplicationTableTbl> newAddTables) {
        Email email = new Email();
        email.setSubject("DRC同步配置变更");
        email.setSender(domainConfig.getDrcConfigSenderEmail());
        email.addCc(domainConfig.getDrcConfigSenderEmail());
        if (domainConfig.getDrcConfigEmailSendSwitch()) {
            if (approvalTbl.getApplicant().endsWith(EMAIL_SUFFIX)) {
                email.addRecipient(approvalTbl.getApplicant());
            } else {
                email.addRecipient(approvalTbl.getApplicant() + EMAIL_SUFFIX);
            }

            domainConfig.getDrcConfigCcEmail().forEach(email::addCc);
            email.addCc(domainConfig.getDrcConfigDbaEmail());
        } else {
            domainConfig.getDrcConfigCcEmail().forEach(email::addRecipient);
        }
        if (applicationForm.getFlushExistingData().equals(BooleanEnum.TRUE.getCode())) {
            email.setHeader("DRC同步已配置，请DBA处理存量数据!</br>");
        } else {
            email.setHeader("DRC同步已配置，请业务验证!</br>");
        }
        email.addContentKeyValue("延迟监控", buildMhaDelayUrl(mhaReplicationDtos));
        email.addContentKeyValue("同步DB", Joiner.on(",").join(dbNames));

        List<String> tableNames = newAddTables.stream().map(ReplicationTableTbl::getTableName).collect(Collectors.toList());
        email.addContentKeyValue("新增表", Joiner.on(",").join(tableNames) + "共" + tableNames.size() + "张表");
        String filterType = applicationForm.getFilterType().equalsIgnoreCase("ALL") ? "无" : applicationForm.getFilterType();
        email.addContentKeyValue("过滤方式", filterType);

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
        PreconditionUtils.checkString(param.getDbName(), "dbName is empty");
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
