package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.log.*;
import com.ctrip.framework.drc.console.dao.log.entity.*;
import com.ctrip.framework.drc.console.enums.*;
import com.ctrip.framework.drc.console.param.log.ConflictApprovalCreateParam;
import com.ctrip.framework.drc.console.param.log.ConflictApprovalQueryParam;
import com.ctrip.framework.drc.console.param.log.ConflictHandleSqlDto;
import com.ctrip.framework.drc.console.param.mysql.MysqlWriteEntity;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.Constants;
import com.ctrip.framework.drc.console.utils.DateUtils;
import com.ctrip.framework.drc.console.vo.log.*;
import com.ctrip.framework.drc.core.monitor.operator.StatementExecutorResult;
import com.ctrip.framework.drc.core.monitor.util.ServicesUtil;
import com.ctrip.framework.drc.core.service.ops.ApprovalApiService;
import com.ctrip.framework.drc.core.service.statistics.traffic.ApprovalApiRequest;
import com.ctrip.framework.drc.core.service.statistics.traffic.ApprovalApiResponse;
import com.ctrip.framework.drc.core.service.user.IAMService;
import com.ctrip.framework.drc.core.service.user.UserService;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
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
import java.util.List;
import java.util.Map;
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
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private DbaApiService dbaApiService;

    private IAMService iamService = ServicesUtil.getIAMService();
    private ApprovalApiService approvalApiService = ApiContainer.getApprovalApiServiceImpl();
    private UserService userService = ApiContainer.getUserServiceImpl();

    private static final String APPROVED = "Approved";
    private static final String REJECTED = "Rejected";

    @Override
    public List<ConflictApprovalView> getConflictApprovalViews(ConflictApprovalQueryParam param) throws Exception {
        resetParam(param);
        List<ConflictAutoHandleBatchTbl> autoHandleBatchTbls = conflictAutoHandleBatchTblDao.queryByParam(param);
        if (CollectionUtils.isEmpty(autoHandleBatchTbls)) {
            return new ArrayList<>();
        }
        param.setBatchIds(autoHandleBatchTbls.stream().map(ConflictAutoHandleBatchTbl::getId).collect(Collectors.toList()));

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

    private void resetParam(ConflictApprovalQueryParam param) {
        Pair<Boolean, List<String>> permissionAndDbsCanQuery = getPermissionAndDbsCanQuery();
        param.setAdmin(permissionAndDbsCanQuery.getLeft());
        param.setDbsWithPermission(permissionAndDbsCanQuery.getRight());
    }

    private Pair<Boolean, List<String>> getPermissionAndDbsCanQuery() {
        if (!iamService.canQueryAllCflLog().getLeft()) {
            List<String> dbsCanQuery = dbaApiService.getDBsWithQueryPermission();
            if (CollectionUtils.isEmpty(dbsCanQuery)) {
                throw ConsoleExceptionUtils.message("no db with DOT permission!");
            }
            return Pair.of(false, dbsCanQuery);
        } else {
            return Pair.of(true, Lists.newArrayList());
        }
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
    public int getWriteSide(Long approvalId) throws Exception {
        ConflictApprovalTbl conflictApprovalTbl = conflictApprovalTblDao.queryById(approvalId);
        if (conflictApprovalTbl == null) {
            throw ConsoleExceptionUtils.message("conflict approval not exist");
        }
        ConflictAutoHandleBatchTbl batchTbl = conflictAutoHandleBatchTblDao.queryById(conflictApprovalTbl.getBatchId());
        return batchTbl.getTargetMhaType();
    }

    @Override
    @DalTransactional(logicDbName = "bbzfxdrclogdb_w")
    public void createConflictApproval(ConflictApprovalCreateParam param) throws Exception {
        List<ConflictHandleSqlDto> handleSqlDtos = param.getHandleSqlDtos();
        if (CollectionUtils.isEmpty(handleSqlDtos)) {
            throw ConsoleExceptionUtils.message("handle sql is empty");
        }

        List<ConflictHandleSqlDto> emptySqlDtos = handleSqlDtos.stream().filter(e -> StringUtils.isBlank(e.getHandleSql())).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(emptySqlDtos)) {
            throw ConsoleExceptionUtils.message("handle sql is empty");
        }

        Pair<ConflictAutoHandleBatchTbl, String> resultPair = insertBatchTbl(handleSqlDtos, param.getWriteSide());
        ConflictAutoHandleBatchTbl batchTbl = resultPair.getLeft();
        Long batchId = batchTbl.getId();
        String dbName = resultPair.getRight();
        String targetMhaName = BooleanEnum.FALSE.getCode().equals(param.getWriteSide()) ? batchTbl.getDstMhaName() : batchTbl.getSrcMhaName();

        MysqlWriteEntity entity = new MysqlWriteEntity(targetMhaName, Constants.CREATE_DRC_WRITE_FILTER, DrcAccountTypeEnum.DRC_CONSOLE.getCode());
        StatementExecutorResult result = mysqlServiceV2.write(entity);
        if (result == null || result.getResult() != SqlResultEnum.SUCCESS.getCode()) {
            throw ConsoleExceptionUtils.message("createConflictApproval fail, " + result.getMessage());
        }

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

        ApprovalApiRequest request;
        if (consoleConfig.getConflictDbOwnerApprovalSwitch()) {
            request = buildRequest(dbOwner, username, approvalTbl.getId());
        } else {
            request = buildRequest(username, username, approvalTbl.getId());
        }

        ApprovalApiResponse response = approvalApiService.createApproval(request);

        approvalTbl.setTicketId(response.getData().get(0).getTicket_ID());
        conflictApprovalTblDao.update(approvalTbl);
    }

    @Override
    public void approvalCallBack(ConflictApprovalCallBackRequest request) throws Exception {
        logger.info("approvalCallBack request: {}", request);
        long approvalId = request.getData().getApprovalId();
        ConflictApprovalTbl conflictApprovalTbl = conflictApprovalTblDao.queryById(approvalId);
        if (REJECTED.equalsIgnoreCase(request.getApprovalStatus())) {
            conflictApprovalTbl.setApprovalResult(ApprovalResultEnum.REJECTED.getCode());
        } else if (APPROVED.equalsIgnoreCase(request.getApprovalStatus())) {
            conflictApprovalTbl.setApprovalResult(ApprovalResultEnum.APPROVED.getCode());
        }
        conflictApprovalTbl.setRemark(request.getRejectReason());

        logger.info("update approval: {}", conflictApprovalTbl);
        conflictApprovalTblDao.update(conflictApprovalTbl);
    }

    @Override
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

        StringBuilder sqlBuilder = new StringBuilder(Constants.BEGIN);
        String insertWriteFilterSql = String.format(Constants.INSERT_DRC_WRITE_FILTER, DrcWriteFilterTypeEnum.CONFLICT.getCode(), approvalTbl.getId(), "");
        sqlBuilder.append("\n").append(insertWriteFilterSql);
        List<String> sqlList = conflictAutoHandleTbls.stream()
                .filter(e -> StringUtils.isNotBlank(e.getAutoHandleSql()))
                .map(e -> Constants.CONFLICT_SQL_PREFIX + e.getAutoHandleSql())
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(sqlList)) {
            throw ConsoleExceptionUtils.message("handler sql is empty");
        }
        String sql = Joiner.on(";\n").join(sqlList);
        sqlBuilder.append("\n").append(sql).append(";\n").append(Constants.COMMIT);

        MysqlWriteEntity entity = new MysqlWriteEntity(targetMha, sqlBuilder.toString(), DrcAccountTypeEnum.DRC_WRITE.getCode());
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
        request.setCcEmail(domainConfig.getConflictApproveCcEmail());
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

    private Pair<ConflictAutoHandleBatchTbl, String> insertBatchTbl(List<ConflictHandleSqlDto> handleSqlDtos, Integer writeSide) throws SQLException {
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
        batchTbl.setId(batchId);
        return Pair.of(batchTbl, dbNames.get(0));
    }
}
