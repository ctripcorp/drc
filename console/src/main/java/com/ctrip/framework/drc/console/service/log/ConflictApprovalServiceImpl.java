package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.dao.log.ConflictApprovalTblDao;
import com.ctrip.framework.drc.console.dao.log.ConflictAutoHandleBatchTblDao;
import com.ctrip.framework.drc.console.dao.log.ConflictAutoHandleTblDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictApprovalTbl;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictAutoHandleTbl;
import com.ctrip.framework.drc.console.param.log.ConflictApprovalQueryParam;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.vo.log.ConflictApprovalView;
import com.ctrip.framework.drc.console.vo.log.ConflictCurrentRecordView;
import com.ctrip.framework.drc.console.vo.log.ConflictRowsLogDetailView;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
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
}
