package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.dao.log.*;
import com.ctrip.framework.drc.console.dao.log.entity.*;
import com.ctrip.framework.drc.console.param.log.ConflictApprovalCreateParam;
import com.ctrip.framework.drc.console.param.log.ConflictApprovalQueryParam;
import com.ctrip.framework.drc.console.param.log.ConflictHandleSqlDto;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.vo.log.ConflictApprovalView;
import com.ctrip.framework.drc.console.vo.log.ConflictAutoHandleView;
import com.ctrip.framework.drc.console.vo.log.ConflictCurrentRecordView;
import com.ctrip.framework.drc.console.vo.log.ConflictRowsLogDetailView;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.base.Joiner;
import org.checkerframework.triplog.shaded.checker.units.qual.C;
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

    @Override
    public List<ConflictApprovalView> getConflictApprovalViews(ConflictApprovalQueryParam param) throws Exception {
        List<ConflictApprovalTbl> conflictApprovalTbls = conflictApprovalTblDao.queryByParam(param);
        if (CollectionUtils.isEmpty(conflictApprovalTbls)) {
            return new ArrayList<>();
        }

        List<ConflictApprovalView> views = conflictApprovalTbls.stream().map(source -> {
            ConflictApprovalView target = new ConflictApprovalView();
            BeanUtils.copyProperties(source, target);
            target.setApprovalId(source.getId());
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
    public List<ConflictRowsLogDetailView> getConflictRowLogDetailView(Long approvalId) throws Exception {
        ConflictApprovalTbl conflictApprovalTbl = conflictApprovalTblDao.queryById(approvalId);
        if (conflictApprovalTbl == null) {
            throw ConsoleExceptionUtils.message("conflict approval not exist");
        }

        List<ConflictAutoHandleTbl> conflictAutoHandleTbls = conflictAutoHandleTblDao.queryByBatchId(conflictApprovalTbl.getBatchId());
        List<Long> rowLogIds = conflictAutoHandleTbls.stream().map(ConflictAutoHandleTbl::getRowLogId).collect(Collectors.toList());
        return conflictLogService.getConflictRowLogDetailView(rowLogIds);
    }

    @Override
    public List<ConflictAutoHandleView> getConflictAutoHandleView(Long batchId) throws Exception {
        List<ConflictAutoHandleTbl> conflictAutoHandleTbls = conflictAutoHandleTblDao.queryByBatchId(batchId);
        if (CollectionUtils.isEmpty(conflictAutoHandleTbls)) {
            return new ArrayList<>();
        }
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
    @DalTransactional(logicDbName = "bbzfxdrclogdb_w")
    public void createConflictApproval(ConflictApprovalCreateParam param) throws Exception {
        List<ConflictHandleSqlDto> handleSqlDtos = param.getHandleSqlDtos();
        Long batchId = insertBatchTbl(handleSqlDtos, param.getWriteSide());
        List<ConflictAutoHandleTbl> autoHandleTbls = handleSqlDtos.stream().map(source -> {
            ConflictAutoHandleTbl target = new ConflictAutoHandleTbl();
            target.setBatchId(batchId);
            target.setRowLogId(source.getRowLogId());
            target.setAutoHandleSql(source.getHandleSql());
            return target;
        }).collect(Collectors.toList());
        conflictAutoHandleTblDao.insert(autoHandleTbls);
    }

    private Long insertBatchTbl(List<ConflictHandleSqlDto> handleSqlDtos, Integer writeSide) throws SQLException {
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

        return conflictAutoHandleBatchTblDao.insertWithReturnId(batchTbl);
    }
}
