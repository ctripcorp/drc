package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.service.impl.ClusterTblServiceImpl;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-13
 */
@RestController
@RequestMapping("/api/drc/v1/clusters/")
public class ClusterController {

    @Autowired
    private ClusterTblServiceImpl clusterTblService;

    @Autowired
    private DalServiceImpl dalService;

    @GetMapping("all/pageNo/{pageNo}/pageSize/{pageSize}")
    public ApiResult getClusters(@PathVariable int pageNo, @PathVariable int pageSize) {
        return ApiResult.getSuccessInstance(clusterTblService.getClusters(pageNo, pageSize));
    }

    @GetMapping("all/count")
    public ApiResult getClusterCount() {
        return ApiResult.getSuccessInstance(clusterTblService.getRecordsCount());
    }

    @GetMapping("{clusterName}/mhas")
    public ApiResult getMhas(@PathVariable String clusterName) {
        return ApiResult.getSuccessInstance(dalService.getMhasFromDal(clusterName));
    }
}
