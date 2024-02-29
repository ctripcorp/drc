package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplicationApprovalTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplicationFormTbl;
import com.ctrip.framework.drc.console.dao.v2.ApplicationApprovalTblDao;
import com.ctrip.framework.drc.console.dao.v2.ApplicationFormTblDao;
import com.ctrip.framework.drc.console.dao.v2.ApplicationRelationTblDao;
import com.ctrip.framework.drc.console.dao.v2.ReplicationTableTblDao;
import com.ctrip.framework.drc.console.dto.v2.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.param.v2.application.ApplicationFormBuildParam;
import com.ctrip.framework.drc.console.param.v2.application.ApplicationFormQueryParam;
import com.ctrip.framework.drc.console.service.v2.impl.DrcApplicationServiceImpl;
import com.ctrip.framework.drc.console.vo.v2.ApplicationFormView;
import com.ctrip.framework.drc.core.service.email.EmailResponse;
import com.ctrip.framework.drc.core.service.email.EmailService;
import com.ctrip.framework.drc.core.service.user.UserService;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2024/2/29 15:38
 */
public class DrcApplicationServiceTest {

    @InjectMocks
    private DrcApplicationServiceImpl drcApplicationService;
    @Mock
    private ApplicationFormTblDao applicationFormTblDao;
    @Mock
    private ApplicationApprovalTblDao applicationApprovalTblDao;
    @Mock
    private ApplicationRelationTblDao applicationRelationTblDao;
    @Mock
    private ReplicationTableTblDao replicationTableTblDao;
    @Mock
    private MhaReplicationServiceV2 mhaReplicationServiceV2;
    @Mock
    private DomainConfig domainConfig;
    @Mock
    private EmailService emailService;
    @Mock
    private UserService userService;


    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testCreateApplicationForm() throws SQLException {
        ApplicationFormBuildParam param = buildParam();
        Mockito.when(applicationFormTblDao.insertWithReturnId(Mockito.any())).thenReturn(1L);
        Mockito.when(applicationApprovalTblDao.insert(Mockito.any(ApplicationApprovalTbl.class))).thenReturn(1);

        drcApplicationService.createApplicationForm(param);
    }

    @Test
    public void testDeleteApplicationForm() throws Exception {
        Mockito.when(applicationFormTblDao.queryById(Mockito.anyLong())).thenReturn(PojoBuilder.buildApplicationFormTbl());
        Mockito.when(applicationApprovalTblDao.queryByApplicationFormId(Mockito.anyLong())).thenReturn(PojoBuilder.buildApplicationApprovalTbl());
        Mockito.when(applicationFormTblDao.update(Mockito.any(ApplicationFormTbl.class))).thenReturn(1);
        Mockito.when(applicationApprovalTblDao.update(Mockito.any(ApplicationApprovalTbl.class))).thenReturn(1);

        drcApplicationService.deleteApplicationForm(1L);
    }

    @Test
    public void testGetApplicationForms() throws Exception {
        ApplicationFormQueryParam param = new ApplicationFormQueryParam();
        param.setApprovalResult(0);

        Mockito.when(applicationApprovalTblDao.queryByApprovalResult(Mockito.anyInt())).thenReturn(Lists.newArrayList(PojoBuilder.buildApplicationApprovalTbl()));
        Mockito.when(applicationFormTblDao.queryByParam(Mockito.any())).thenReturn(Lists.newArrayList(PojoBuilder.buildApplicationFormTbl()));
        Mockito.when(applicationApprovalTblDao.queryByApplicationFormIds(Mockito.anyList())).thenReturn(Lists.newArrayList(PojoBuilder.buildApplicationApprovalTbl()));

        List<ApplicationFormView> result = drcApplicationService.getApplicationForms(param);
        Assert.assertEquals(result.size(), 1);
    }

    @Test
    public void testApproveForm() throws Exception {
        Mockito.when(applicationApprovalTblDao.queryByApplicationFormId(Mockito.anyLong())).thenReturn(PojoBuilder.buildApplicationApprovalTbl());
        Mockito.when(userService.getInfo()).thenReturn("username");
        Mockito.when(applicationApprovalTblDao.update(Mockito.any(ApplicationApprovalTbl.class))).thenReturn(1);

        drcApplicationService.approveForm(1L);
    }

    @Test
    public void testSendEmail() throws Exception {
        Mockito.when(applicationFormTblDao.queryById(Mockito.anyLong())).thenReturn(PojoBuilder.buildApplicationFormTbl());
        Mockito.when(applicationApprovalTblDao.queryByApplicationFormId(Mockito.anyLong())).thenReturn(PojoBuilder.buildApplicationApprovalTbl1());
        Mockito.when(applicationRelationTblDao.queryByApplicationFormId(Mockito.anyLong())).thenReturn(Lists.newArrayList(PojoBuilder.buildApplicationRelationTbl()));
        Mockito.when(replicationTableTblDao.queryExistByDbReplicationIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(Lists.newArrayList(PojoBuilder.buildReplicationTableTbl()));
        Mockito.when(mhaReplicationServiceV2.getMhaReplicationDelays(Mockito.anyList())).thenReturn(buildMhaDelayInfoDtos());
        Mockito.when(applicationFormTblDao.update(Mockito.any(ApplicationFormTbl.class))).thenReturn(1);
        Mockito.when(replicationTableTblDao.update(Mockito.anyList())).thenReturn(new int[1]);

        Mockito.when(domainConfig.getDrcConfigSenderEmail()).thenReturn("senderEmail");
        Mockito.when(domainConfig.getDrcConfigEmailSendSwitch()).thenReturn(true);
        Mockito.when(domainConfig.getDrcConfigCcEmail()).thenReturn(Lists.newArrayList("cc"));
        Mockito.when(domainConfig.getDrcConfigDbaEmail()).thenReturn("dba");

        EmailResponse response = new EmailResponse();
        response.setSuccess(true);
        Mockito.when(emailService.sendEmail(Mockito.any())).thenReturn(response);

        boolean result = drcApplicationService.sendEmail(1L);
        Assert.assertTrue(result);

    }

    private List<MhaDelayInfoDto> buildMhaDelayInfoDtos() {
        MhaDelayInfoDto mhaDelayInfoDto = new MhaDelayInfoDto();
        mhaDelayInfoDto.setSrcMha("srcMha");
        mhaDelayInfoDto.setDstMha("dstMha");
        mhaDelayInfoDto.setSrcTime(2L);
        mhaDelayInfoDto.setDstTime(1L);

        return Lists.newArrayList(mhaDelayInfoDto);
    }

    private ApplicationFormBuildParam buildParam() {
        ApplicationFormBuildParam param = new ApplicationFormBuildParam();
        param.setBuName("buName");
        param.setDbName("db");
        param.setTableName("table");
        param.setSrcRegion("srcRegion");
        param.setDstRegion("dstRegion");
        param.setTps("tps");
        param.setDescription("desc");
        param.setDisruptionImpact("impact");
        param.setFilterType("ALL");
        param.setTag("tag");
        param.setFlushExistingData(1);
        param.setOrderRelated(1);
        param.setGtidInit("gtid");
        param.setApplicant("applicant");
        param.setRemark("remark");

        return param;
    }
}