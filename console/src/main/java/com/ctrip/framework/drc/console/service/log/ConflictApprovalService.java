package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.param.log.ConflictApprovalQueryParam;
import com.ctrip.framework.drc.console.vo.log.ConflictApprovalView;
import com.ctrip.framework.drc.console.vo.log.ConflictAutoHandleView;
import com.ctrip.framework.drc.console.vo.log.ConflictCurrentRecordView;
import com.ctrip.framework.drc.console.vo.log.ConflictRowsLogDetailView;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/10/31 11:19
 */
public interface ConflictApprovalService {

    List<ConflictApprovalView> getConflictApprovalViews(ConflictApprovalQueryParam param) throws Exception;

    ConflictCurrentRecordView getConflictRecordView(Long approvalId) throws Exception;

    List<ConflictRowsLogDetailView> getConflictRowLogDetailView(Long approvalId) throws Exception;

    List<ConflictAutoHandleView> getConflictAutoHandleView(Long batchId) throws Exception;
}
