package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.param.v2.ColumnsFilterCreateParam;
import com.ctrip.framework.drc.console.param.v2.DbReplicationBuildParam;
import com.ctrip.framework.drc.console.param.v2.DrcBuildParam;
import com.ctrip.framework.drc.console.param.v2.DrcMhaBuildParam;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/7/27 14:50
 */
public interface DrcBuildServiceV2 {

    void buildMha(DrcMhaBuildParam param) throws Exception;

    void buildDrc(DrcBuildParam param) throws Exception;

    List<Long> configureDbReplications(DbReplicationBuildParam param) throws Exception;

    void deleteDbReplications(List<Long> dbReplicationIds) throws Exception;

    void buildColumnsFilter(ColumnsFilterCreateParam param) throws Exception;

    void deleteColumnsFilter(List<Long> dbReplicationIds) throws Exception;
}
