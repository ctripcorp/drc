package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.param.v2.*;
import com.ctrip.framework.drc.console.vo.v2.DbReplicationView;
import com.ctrip.framework.drc.console.vo.v2.DrcConfigView;
import com.ctrip.framework.drc.console.vo.v2.DrcMhaApplierView;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/7/27 14:50
 */
public interface DrcBuildServiceV2 {

    void buildMha(DrcMhaBuildParam param) throws Exception;

    String buildDrc(DrcBuildParam param) throws Exception;

    List<Long> configureDbReplications(DbReplicationBuildParam param) throws Exception;

    List<DbReplicationView> getDbReplicationView(String srcMhaName, String dstMhaName) throws Exception;

    void deleteDbReplications(List<Long> dbReplicationIds) throws Exception;

    void buildColumnsFilter(ColumnsFilterCreateParam param) throws Exception;

    void deleteColumnsFilter(List<Long> dbReplicationIds) throws Exception;

    void buildRowsFilter(RowsFilterCreateParam param) throws Exception;

    void deleteRowsFilter(List<Long> dbReplicationIds) throws Exception;

    DrcConfigView getDrcConfigView(String srcMhaName, String dstMhaName) throws Exception;

    DrcMhaApplierView getDrcMhaApplierView(String srcMhaName, String dstMhaName) throws Exception;

    List<String> getMhaAppliers(String srcMhaName, String dstMhaName) throws Exception;

    String getApplierGtid(String srcMhaName, String dstMhaName) throws Exception;
}
