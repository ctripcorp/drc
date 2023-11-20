package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.log.*;
import com.ctrip.framework.drc.console.dao.log.entity.*;
import com.ctrip.framework.drc.console.enums.ApprovalResultEnum;
import com.ctrip.framework.drc.console.enums.ApprovalTypeEnum;
import com.ctrip.framework.drc.console.param.log.ConflictApprovalCreateParam;
import com.ctrip.framework.drc.console.param.log.ConflictApprovalQueryParam;
import com.ctrip.framework.drc.console.param.log.ConflictHandleSqlDto;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.vo.log.*;
import com.ctrip.framework.drc.core.monitor.operator.StatementExecutorResult;
import com.ctrip.framework.drc.core.service.ops.ApprovalApiService;
import com.ctrip.framework.drc.core.service.statistics.traffic.ApprovalApiResponse;
import com.ctrip.framework.drc.core.service.user.UserService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.Timestamp;
import java.util.List;

import static com.ctrip.framework.drc.console.service.v2.MetaGeneratorBuilder.getDbTbls;

/**
 * Created by dengquanliang
 * 2023/11/15 20:10
 */
public class ConflictApprovalServiceTest {
    
    @InjectMocks
    private ConflictApprovalServiceImpl conflictApprovalService;

    @Mock
    private ConflictApprovalTblDao conflictApprovalTblDao;
    @Mock
    private ConflictAutoHandleBatchTblDao conflictAutoHandleBatchTblDao;
    @Mock
    private ConflictAutoHandleTblDao conflictAutoHandleTblDao;
    @Mock
    private ConflictRowsLogTblDao conflictRowsLogTblDao;
    @Mock
    private ConflictTrxLogTblDao conflictTrxLogTblDao;
    @Mock
    private ConflictLogService conflictLogService;
    @Mock
    private DomainConfig domainConfig;
    @Mock
    private MysqlServiceV2 mysqlServiceV2;
    @Mock
    private ApprovalApiService approvalApiService;
    @Mock
    private UserService userService;
    @Mock
    private DbTblDao dbTblDao;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetConflictApprovalViews() throws Exception {
        ConflictApprovalQueryParam param = new ConflictApprovalQueryParam();
        Mockito.when(conflictApprovalTblDao.queryByParam(param)).thenReturn(buildApprovalTbls());
        Mockito.when(conflictAutoHandleBatchTblDao.queryByIds(Mockito.anyList())).thenReturn(buildBatchTbls());
        Mockito.when(domainConfig.getApprovalDetailUrl()).thenReturn("detail url");

        List<ConflictApprovalView> results = conflictApprovalService.getConflictApprovalViews(param);
        Assert.assertEquals(results.size(), 1);

    }

    @Test
    public void testGetConflictRecordView() throws Exception {
        ConflictCurrentRecordView view = new ConflictCurrentRecordView();
        view.setRecordIsEqual(true);

        Mockito.when(conflictApprovalTblDao.queryById(Mockito.anyLong())).thenReturn(buildApprovalTbls().get(0));
        Mockito.when(conflictAutoHandleTblDao.queryByBatchId(Mockito.anyLong())).thenReturn(buildHandleTbls());
        Mockito.when(conflictLogService.getConflictRowRecordView(Mockito.anyList())).thenReturn(view);

        ConflictCurrentRecordView result = conflictApprovalService.getConflictRecordView(1L);
        Assert.assertTrue(result.isRecordIsEqual());
    }

    @Test
    public void testGetConflictRowLogDetailView() throws Exception {
        ConflictTrxLogDetailView view = new ConflictTrxLogDetailView();
        Mockito.when(conflictApprovalTblDao.queryById(Mockito.anyLong())).thenReturn(buildApprovalTbls().get(0));
        Mockito.when(conflictAutoHandleTblDao.queryByBatchId(Mockito.anyLong())).thenReturn(buildHandleTbls());
        Mockito.when(conflictLogService.getRowLogDetailView(Mockito.anyList())).thenReturn(view);

        ConflictTrxLogDetailView result = conflictApprovalService.getConflictRowLogDetailView(1L);
        Assert.assertNotNull(result);
    }

    @Test
    public void testGetConflictAutoHandleView() throws Exception {
        Mockito.when(conflictApprovalTblDao.queryById(Mockito.anyLong())).thenReturn(buildApprovalTbls().get(0));
        Mockito.when(conflictAutoHandleTblDao.queryByBatchId(Mockito.anyLong())).thenReturn(buildHandleTbls());
        Mockito.when(conflictRowsLogTblDao.queryByIds(Mockito.anyList())).thenReturn(buildConflictRowsLogTbls());

        List<ConflictAutoHandleView> result = conflictApprovalService.getConflictAutoHandleView(1L);
        Assert.assertEquals(result.size(), 1);
    }

    @Test
    public void testCreateConflictApproval() throws Exception {
        ConflictApprovalCreateParam param = new ConflictApprovalCreateParam();
        ConflictHandleSqlDto sqlDto = new ConflictHandleSqlDto();
        sqlDto.setRowLogId(1L);
        sqlDto.setHandleSql("sql");
        param.setWriteSide(0);
        param.setHandleSqlDtos(Lists.newArrayList(sqlDto));

        Mockito.when(conflictRowsLogTblDao.queryByIds(Mockito.anyList())).thenReturn(buildConflictRowsLogTbls());
        Mockito.when(conflictTrxLogTblDao.queryByIds(Mockito.anyList())).thenReturn(buildConflictTrxLogTbls());
        Mockito.when(conflictAutoHandleBatchTblDao.insertWithReturnId(Mockito.any())).thenReturn(1L);
        Mockito.when(conflictAutoHandleTblDao.insert(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(userService.getInfo()).thenReturn("username");
        Mockito.when(dbTblDao.queryByDbNames(Mockito.anyList())).thenReturn(getDbTbls());
        Mockito.when(conflictApprovalTblDao.insertWithReturnId(Mockito.any())).thenReturn(1L);

        ApprovalApiResponse response = new ApprovalApiResponse();
        ApprovalApiResponse.ResponseData data = new ApprovalApiResponse.ResponseData();
        data.setTicket_ID("ticketId");
        response.setData(Lists.newArrayList(data));
        Mockito.when(approvalApiService.createApproval(Mockito.any())).thenReturn(response);
        Mockito.when(conflictApprovalTblDao.update(Mockito.any(ConflictApprovalTbl.class))).thenReturn(1);

        conflictApprovalService.createConflictApproval(param);
        Mockito.verify(conflictApprovalTblDao, Mockito.times(1)).insertWithReturnId(Mockito.any());
        Mockito.verify(conflictApprovalTblDao, Mockito.times(1)).update(Mockito.any(ConflictApprovalTbl.class));
        Mockito.verify(conflictAutoHandleBatchTblDao, Mockito.times(1)).insertWithReturnId(Mockito.any());
        Mockito.verify(conflictAutoHandleTblDao, Mockito.times(1)).insert(Mockito.anyList());
    }

    @Test
    public void testApprovalCallBack() throws Exception {
        ConflictApprovalCallBackRequest request = new ConflictApprovalCallBackRequest();
        ConflictApprovalCallBackRequest.Data data = new ConflictApprovalCallBackRequest.Data();
        data.setApprovalId(1L);
        request.setApprovalStatus("Approved");
        request.setData(data);

        Mockito.when(conflictApprovalTblDao.queryById(Mockito.anyLong())).thenReturn(buildApprovalTbls().get(0));
        Mockito.when(conflictApprovalTblDao.update(Mockito.any(ConflictApprovalTbl.class))).thenReturn(1);

        conflictApprovalService.approvalCallBack(request);
        Mockito.verify(conflictApprovalTblDao, Mockito.times(1)).update(Mockito.any(ConflictApprovalTbl.class));
    }

    @Test
    public void testExecuteApproval() throws Exception {
        Mockito.when(conflictApprovalTblDao.queryById(Mockito.anyLong())).thenReturn(buildApprovalTbls().get(0));
        Mockito.when(conflictAutoHandleBatchTblDao.queryById(Mockito.anyLong())).thenReturn(buildBatchTbls().get(0));
        Mockito.when(conflictAutoHandleTblDao.queryByBatchId(Mockito.anyLong())).thenReturn(buildHandleTbls());
        Mockito.when(mysqlServiceV2.write(Mockito.any())).thenReturn(new StatementExecutorResult(1, "success"));
        Mockito.when(conflictAutoHandleBatchTblDao.update(Mockito.any(ConflictAutoHandleBatchTbl.class))).thenReturn(1);

        conflictApprovalService.executeApproval(1L);
        Mockito.verify(conflictAutoHandleBatchTblDao, Mockito.times(1)).update(Mockito.any(ConflictAutoHandleBatchTbl.class));

    }

    private List<ConflictTrxLogTbl> buildConflictTrxLogTbls() {
        ConflictTrxLogTbl tbl = new ConflictTrxLogTbl();
        tbl.setId(1L);
        tbl.setTrxResult(0);
        tbl.setSrcMhaName("srcMha");
        tbl.setDstMhaName("dstMha");
        tbl.setGtid("gtid");
        tbl.setTrxRowsNum(1L);
        tbl.setCflRowsNum(1L);
        tbl.setHandleTime(System.currentTimeMillis());
        return Lists.newArrayList(tbl);
    }

    private List<ConflictRowsLogTbl> buildConflictRowsLogTbls() {
        ConflictRowsLogTbl tbl = new ConflictRowsLogTbl();
        tbl.setId(1L);
        tbl.setConflictTrxLogId(1L);
        tbl.setDbName("dbName");
        tbl.setTableName("tableName");
        tbl.setRawSql("rawSql");
        tbl.setRawSqlResult("rawSqlResult");
        tbl.setDstRowRecord("dstRowRecord");
        tbl.setHandleSql("handleSql");
        tbl.setHandleSqlResult("handleSqlResult");
        tbl.setRowResult(0);
        tbl.setHandleTime(System.currentTimeMillis());
        tbl.setRowId(1L);

        return Lists.newArrayList(tbl);
    }

    private List<ConflictApprovalTbl> buildApprovalTbls() {
        ConflictApprovalTbl tbl = new ConflictApprovalTbl();
        tbl.setBatchId(1L);
        tbl.setApprovalResult(ApprovalResultEnum.APPROVED.getCode());
        tbl.setCreateTime(new Timestamp(System.currentTimeMillis()));
        tbl.setCurrentApproverType(ApprovalTypeEnum.DB_OWNER.getCode());

        return Lists.newArrayList(tbl);
    }

    private List<ConflictAutoHandleBatchTbl> buildBatchTbls() {
        ConflictAutoHandleBatchTbl tbl = new ConflictAutoHandleBatchTbl();
        tbl.setSrcMhaName("srcMha");
        tbl.setDstMhaName("dstMha");
        tbl.setTargetMhaType(0);
        tbl.setDbName("db");
        tbl.setTableName("table");
        tbl.setStatus(0);

        return Lists.newArrayList(tbl);
    }

    private List<ConflictAutoHandleTbl> buildHandleTbls() {
        ConflictAutoHandleTbl tbl = new ConflictAutoHandleTbl();
        tbl.setAutoHandleSql("sql");
        tbl.setBatchId(1L);
        tbl.setRowLogId(1L);

        return Lists.newArrayList(tbl);
    }


    
}
