package com.ctrip.framework.drc.applier.activity.monitor;

import com.ctrip.framework.drc.applier.utils.ApplierDynamicConfig;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.drc.fetcher.activity.monitor.ReportActivity;
import com.ctrip.framework.drc.fetcher.conflict.ConflictTransactionLog;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.google.common.collect.Lists;
import org.springframework.http.*;
import org.springframework.web.client.RestOperations;

import java.util.List;

/**
 * Created by jixinwang on 2020/10/14
 */
public class ReportConflictActivity extends ReportActivity<ConflictTransactionLog, Boolean> {

    @InstanceConfig(path = "cluster")
    public String cluster = "unset";

    @InstanceConfig(path = "replicator.mhaName")
    public String srcMhaName = "unset";

    @InstanceConfig(path = "target.mhaName")
    public String destMhaName = "unset";
    
    public String conflictLogUploadUrl = ApplierDynamicConfig.getInstance().getConflictLogUploadUrl();
    
    @Override
    public void doReport(List<ConflictTransactionLog> taskList) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Lists.newArrayList(MediaType.APPLICATION_JSON));
        HttpEntity<Object> entity = new HttpEntity<Object>(taskList, headers);
        restTemplate.exchange(conflictLogUploadUrl, HttpMethod.POST, entity, ApiResult.class);
    }

    @Override
    public boolean report(ConflictTransactionLog conflictTransactionLog) {
         if ("on".equals(ApplierDynamicConfig.getInstance().getConflictLogUploadSwitch())) {
            conflictTransactionLog.setSrcMha(srcMhaName);
            conflictTransactionLog.setDstMha(destMhaName);
            conflictTransactionLog.setHandleTime(System.currentTimeMillis());
            return trySubmit(conflictTransactionLog);
        }
        return true;
    }

    public void setRestTemplate(RestOperations restTemplate) {
        this.restTemplate = restTemplate;
    }
}
