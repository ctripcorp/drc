package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.log.*;
import com.ctrip.framework.drc.console.dao.log.entity.*;
import com.ctrip.framework.drc.console.enums.ApprovalResultEnum;
import com.ctrip.framework.drc.console.enums.ApprovalTypeEnum;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.SqlResultEnum;
import com.ctrip.framework.drc.console.param.log.ConflictApprovalCreateParam;
import com.ctrip.framework.drc.console.param.log.ConflictApprovalQueryParam;
import com.ctrip.framework.drc.console.param.log.ConflictHandleSqlDto;
import com.ctrip.framework.drc.console.param.mysql.MysqlWriteEntity;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.Constants;
import com.ctrip.framework.drc.console.utils.DateUtils;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.console.vo.log.*;
import com.ctrip.framework.drc.core.monitor.operator.StatementExecutorResult;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.ops.ApprovalApiService;
import com.ctrip.framework.drc.core.service.statistics.traffic.ApprovalApiRequest;
import com.ctrip.framework.drc.core.service.statistics.traffic.ApprovalApiResponse;
import com.ctrip.framework.drc.core.service.user.UserService;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/10/31 11:21
 */
@Service
public class ConflictApprovalServiceImpl implements ConflictApprovalService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConflictApprovalTblDao conflictApprovalTblDao;
    @Autowired
    private ConflictAutoHandleBatchTblDao conflictAutoHandleBatchTblDao;
    @Autowired
    private ConflictAutoHandleTblDao conflictAutoHandleTblDao;
    @Autowired
    private ConflictRowsLogTblDao conflictRowsLogTblDao;
    @Autowired
    private ConflictTrxLogTblDao conflictTrxLogTblDao;
    @Autowired
    private ConflictLogService conflictLogService;
    @Autowired
    private DomainConfig domainConfig;
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;
    @Autowired
    private DbTblDao dbTblDao;

    private ApprovalApiService approvalApiService = ApiContainer.getApprovalApiServiceImpl();
    private UserService userService = ApiContainer.getUserServiceImpl();

    private static final String APPROVED = "Approved";
    private static final String REJECTED = "Rejected";

    @Override
    public List<ConflictApprovalView> getConflictApprovalViews(ConflictApprovalQueryParam param) throws Exception {
        if (StringUtils.isNotBlank(param.getDbName()) || StringUtils.isNotBlank(param.getTableName())) {
            List<ConflictAutoHandleBatchTbl> batchTbls = conflictAutoHandleBatchTblDao.queryByDb(param.getDbName(), param.getTableName());
            if (CollectionUtils.isEmpty(batchTbls)) {
                return new ArrayList<>();
            }
            List<Long> batchIds = batchTbls.stream().map(ConflictAutoHandleBatchTbl::getId).collect(Collectors.toList());
            param.setBatchIds(batchIds);
        }

        List<ConflictApprovalTbl> conflictApprovalTbls = conflictApprovalTblDao.queryByParam(param);
        if (CollectionUtils.isEmpty(conflictApprovalTbls)) {
            return new ArrayList<>();
        }

        List<Long> batchIds = conflictApprovalTbls.stream().map(ConflictApprovalTbl::getBatchId).collect(Collectors.toList());
        List<ConflictAutoHandleBatchTbl> batchTbls = conflictAutoHandleBatchTblDao.queryByIds(batchIds);
        Map<Long, ConflictAutoHandleBatchTbl> batchTblMap = batchTbls.stream().collect(Collectors.toMap(ConflictAutoHandleBatchTbl::getId, Function.identity()));

        String approvalDetailUrl = domainConfig.getApprovalDetailUrl();
        List<ConflictApprovalView> views = conflictApprovalTbls.stream().map(source -> {
            ConflictApprovalView target = new ConflictApprovalView();
            BeanUtils.copyProperties(source, target, "createTime");
            target.setApprovalId(source.getId());

            String createTime = DateUtils.longToString(source.getCreateTime().getTime());
            target.setCreateTime(createTime);
            target.setApprovalDetailUrl(approvalDetailUrl + source.getTicketId());
            ConflictAutoHandleBatchTbl batchTbl = batchTblMap.get(source.getBatchId());
            if (batchTbl != null) {
                target.setDbName(batchTbl.getDbName());
                target.setTableName(batchTbl.getTableName());
                target.setExecutedStatus(batchTbl.getStatus());
            }
            return target;
        }).collect(Collectors.toList());
        return views;
    }

    @Override
    public ConflictCurrentRecordView getConflictRecordView(Long approvalId) throws Exception {
        ConflictApprovalTbl conflictApprovalTbl = conflictApprovalTblDao.queryById(approvalId);
        if (conflictApprovalTbl == null) {
            throw ConsoleExceptionUtils.message("conflict approval not exist");
        }

        List<ConflictAutoHandleTbl> conflictAutoHandleTbls = conflictAutoHandleTblDao.queryByBatchId(conflictApprovalTbl.getBatchId());
        List<Long> rowLogIds = conflictAutoHandleTbls.stream().map(ConflictAutoHandleTbl::getRowLogId).collect(Collectors.toList());

        return conflictLogService.getConflictRowRecordView(rowLogIds);
    }

    @Override
    public ConflictTrxLogDetailView getConflictRowLogDetailView(Long approvalId) throws Exception {
        ConflictApprovalTbl conflictApprovalTbl = conflictApprovalTblDao.queryById(approvalId);
        if (conflictApprovalTbl == null) {
            throw ConsoleExceptionUtils.message("conflict approval not exist");
        }

        List<ConflictAutoHandleTbl> conflictAutoHandleTbls = conflictAutoHandleTblDao.queryByBatchId(conflictApprovalTbl.getBatchId());
        List<Long> rowLogIds = conflictAutoHandleTbls.stream().map(ConflictAutoHandleTbl::getRowLogId).collect(Collectors.toList());
        return conflictLogService.getRowLogDetailView(rowLogIds);
    }

    @Override
    public List<ConflictAutoHandleView> getConflictAutoHandleView(Long approvalId) throws Exception {
        ConflictApprovalTbl conflictApprovalTbl = conflictApprovalTblDao.queryById(approvalId);
        if (conflictApprovalTbl == null) {
            throw ConsoleExceptionUtils.message("conflict approval not exist");
        }

        List<ConflictAutoHandleTbl> conflictAutoHandleTbls = conflictAutoHandleTblDao.queryByBatchId(conflictApprovalTbl.getBatchId());
        List<Long> rowLogIds = conflictAutoHandleTbls.stream().map(ConflictAutoHandleTbl::getRowLogId).collect(Collectors.toList());
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByIds(rowLogIds);
        Map<Long, ConflictRowsLogTbl> rowsLogTblMap = conflictRowsLogTbls.stream().collect(Collectors.toMap(ConflictRowsLogTbl::getId, Function.identity()));

        List<ConflictAutoHandleView> views = conflictAutoHandleTbls.stream().map(source -> {
            ConflictAutoHandleView target = new ConflictAutoHandleView();
            target.setAutoHandleSql(source.getAutoHandleSql());
            target.setRowLogId(source.getRowLogId());
            ConflictRowsLogTbl rowsLogTbl = rowsLogTblMap.get(source.getRowLogId());
            if (rowsLogTbl != null) {
                target.setDbName(rowsLogTbl.getDbName());
                target.setTableName(rowsLogTbl.getTableName());
            }
            return target;
        }).collect(Collectors.toList());
        return views;
    }

    @Override
    public void createConflictApproval(ConflictApprovalCreateParam param) throws Exception {
        List<ConflictHandleSqlDto> handleSqlDtos = param.getHandleSqlDtos();
        Pair<Long, String> resultPair = insertBatchTbl(handleSqlDtos, param.getWriteSide());
        Long batchId = resultPair.getLeft();
        String dbName = resultPair.getRight();

        List<ConflictAutoHandleTbl> autoHandleTbls = handleSqlDtos.stream().map(source -> {
            ConflictAutoHandleTbl target = new ConflictAutoHandleTbl();
            target.setBatchId(batchId);
            target.setRowLogId(source.getRowLogId());
            target.setAutoHandleSql(source.getHandleSql());
            return target;
        }).collect(Collectors.toList());
        conflictAutoHandleTblDao.insert(autoHandleTbls);

        String username = userService.getInfo();
        ConflictApprovalTbl approvalTbl = insertApprovalTbl(batchId, username);

        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(Lists.newArrayList(dbName));
        if (CollectionUtils.isEmpty(dbTbls)) {
            throw ConsoleExceptionUtils.message(String.format("db: %s not exist", dbName));
        }
        String dbOwner = dbTbls.get(0).getDbOwner();

        //ql_deng TODO 2023/11/16:dbOwner
        ApprovalApiRequest request = buildRequest(username, username, approvalTbl.getId());
        ApprovalApiResponse response = approvalApiService.createApproval(request);

        approvalTbl.setTicketId(response.getData().get(0).getTicket_ID());
        conflictApprovalTblDao.update(approvalTbl);
    }

    @Override
    public void approvalCallBack(ConflictApprovalCallBackRequest request) throws Exception {
        logger.info("approvalCallBack request: ", request);
        long approvalId = request.getData().getApprovalId();
        ConflictApprovalTbl conflictApprovalTbl = conflictApprovalTblDao.queryById(approvalId);
        if (REJECTED.equalsIgnoreCase(request.getApprovalStatus())) {
            conflictApprovalTbl.setApprovalResult(ApprovalResultEnum.REJECTED.getCode());
            conflictApprovalTbl.setRemark(request.getRejectReason());
        } else if (APPROVED.equalsIgnoreCase(request.getApprovalStatus())) {
            if (ApprovalTypeEnum.DB_OWNER.getCode() == conflictApprovalTbl.getCurrentApproverType()) {
                conflictApprovalTbl.setCurrentApproverType(ApprovalTypeEnum.DBA.getCode());
            } else if (ApprovalTypeEnum.DBA.getCode() == conflictApprovalTbl.getCurrentApproverType()) {
                conflictApprovalTbl.setApprovalResult(ApprovalResultEnum.APPROVED.getCode());
            }
        }

        conflictApprovalTblDao.update(conflictApprovalTbl);
    }

    @Override
    @DalTransactional(logicDbName = "bbzfxdrclogdb_w")
    public void executeApproval(Long approvalId) throws Exception {
        ConflictApprovalTbl approvalTbl = conflictApprovalTblDao.queryById(approvalId);
        if (approvalTbl == null) {
            throw ConsoleExceptionUtils.message("approval not exist");
        }
        if (approvalTbl.getApprovalResult() != ApprovalResultEnum.APPROVED.getCode()) {
            throw ConsoleExceptionUtils.message("approval has not been approved yet!");
        }

        ConflictAutoHandleBatchTbl batchTbl = conflictAutoHandleBatchTblDao.queryById(approvalTbl.getBatchId());
        if (batchTbl.getStatus().equals(BooleanEnum.TRUE.getCode())) {
            throw ConsoleExceptionUtils.message("approval has already been executed");
        }

        String targetMha = batchTbl.getTargetMhaType() == 0 ? batchTbl.getDstMhaName() : batchTbl.getSrcMhaName();
        List<ConflictAutoHandleTbl> conflictAutoHandleTbls = conflictAutoHandleTblDao.queryByBatchId(batchTbl.getId());

        //ql_deng TODO 2023/11/16:drc filter sql
        StringBuilder sqlBuilder = new StringBuilder(Constants.BEGIN);
        List<String> sqlList = conflictAutoHandleTbls.stream().map(e -> Constants.CONFLICT_SQL_PREFIX + e.getAutoHandleSql()).collect(Collectors.toList());
        String sql = Joiner.on(";\n").join(sqlList);
        sqlBuilder.append("\n").append(sql).append(";\n").append(Constants.COMMIT);

        MysqlWriteEntity entity = new MysqlWriteEntity(targetMha, sqlBuilder.toString());
        StatementExecutorResult result = mysqlServiceV2.write(entity);
        batchTbl.setStatus(result.getResult());
        batchTbl.setRemark(result.getMessage());
        conflictAutoHandleBatchTblDao.update(batchTbl);
        if (result.getResult() != SqlResultEnum.SUCCESS.getCode()) {
            throw ConsoleExceptionUtils.message(result.getMessage());
        }
    }

    private ApprovalApiRequest buildRequest(String dbOwner, String username, long approvalId) {
        ApprovalApiRequest request = new ApprovalApiRequest();
        request.setUrl(domainConfig.getOpsApprovalUrl());

        String sourceUrl = domainConfig.getConflictDetailUrl() + "?approvalId=" + approvalId + "&queryType=2";
        request.setSourceUrl(sourceUrl);
        request.setApprover1(dbOwner);
        request.setApprover2(domainConfig.getDbaApprovers());
        request.setCcEmail(domainConfig.getConflictCcEmail());
        request.setCallBackUrl(domainConfig.getApprovalCallbackUrl());
        request.setUsername(username);

        ConflictApprovalCallBackRequest.Data data = new ConflictApprovalCallBackRequest.Data();
        data.setApprovalId(approvalId);
        request.setData(JsonUtils.toJson(data));
        request.setToken(domainConfig.getOpsApprovalToken());

        return request;
    }

    private ConflictApprovalTbl insertApprovalTbl(Long batchId, String username) throws SQLException {
        ConflictApprovalTbl approvalTbl = new ConflictApprovalTbl();
        approvalTbl.setBatchId(batchId);
        approvalTbl.setApprovalResult(ApprovalResultEnum.UNDER_APPROVAL.getCode());
        approvalTbl.setApplicant(username);
        approvalTbl.setTicketId("");
        approvalTbl.setCurrentApproverType(ApprovalTypeEnum.DB_OWNER.getCode());
        Long approvalId = conflictApprovalTblDao.insertWithReturnId(approvalTbl);

        approvalTbl.setId(approvalId);
        return approvalTbl;
    }

    private Pair<Long, String> insertBatchTbl(List<ConflictHandleSqlDto> handleSqlDtos, Integer writeSide) throws SQLException {
        List<Long> rowLogIds = handleSqlDtos.stream().map(ConflictHandleSqlDto::getRowLogId).collect(Collectors.toList());
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByIds(rowLogIds);
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            throw ConsoleExceptionUtils.message("rowLogs not exist");
        }

        List<String> dbNames = conflictRowsLogTbls.stream().map(ConflictRowsLogTbl::getDbName).distinct().collect(Collectors.toList());
        if (dbNames.size() != 1) {
            throw ConsoleExceptionUtils.message("can not select different db");
        }
        List<String> tableNames = conflictRowsLogTbls.stream().map(ConflictRowsLogTbl::getTableName).distinct().collect(Collectors.toList());

        List<Long> trxLogIds = conflictRowsLogTbls.stream().map(ConflictRowsLogTbl::getConflictTrxLogId).distinct().collect(Collectors.toList());
        List<ConflictTrxLogTbl> conflictTrxLogTbls = conflictTrxLogTblDao.queryByIds(trxLogIds);
        if (CollectionUtils.isEmpty(conflictTrxLogTbls)) {
            throw ConsoleExceptionUtils.message("trxLogs not exist");
        }
        List<String> srcMhaNames = conflictTrxLogTbls.stream().map(ConflictTrxLogTbl::getSrcMhaName).distinct().collect(Collectors.toList());
        List<String> dstMhaNames = conflictTrxLogTbls.stream().map(ConflictTrxLogTbl::getDstMhaName).distinct().collect(Collectors.toList());

        if (srcMhaNames.size() != 1 || dstMhaNames.size() != 1) {
            throw ConsoleExceptionUtils.message("selected rowLogs belong to different mhaReplication");
        }

        String dbName = dbNames.get(0);
        String tableName = Joiner.on(",").join(tableNames);
        String srcMhaName = srcMhaNames.get(0);
        String dstMhaName = dstMhaNames.get(0);

        ConflictAutoHandleBatchTbl batchTbl = new ConflictAutoHandleBatchTbl();
        batchTbl.setSrcMhaName(srcMhaName);
        batchTbl.setDstMhaName((dstMhaName));
        batchTbl.setTargetMhaType(writeSide);
        batchTbl.setDbName(dbName);
        batchTbl.setTableName(tableName);
        batchTbl.setStatus(SqlResultEnum.NOT_EXECUTED.getCode());

        Long batchId = conflictAutoHandleBatchTblDao.insertWithReturnId(batchTbl);
        return Pair.of(batchId, dbNames.get(0));
    }
}
