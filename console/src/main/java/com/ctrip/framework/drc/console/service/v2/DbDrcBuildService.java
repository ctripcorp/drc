package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dto.v3.DbApplierDto;
import com.ctrip.framework.drc.console.param.v2.DrcBuildBaseParam;
import com.ctrip.framework.drc.console.param.v2.DrcBuildParam;

import java.util.List;

public interface DbDrcBuildService {

    List<DbApplierDto> getMhaDbAppliers(String srcMhaName, String dstMhaName) throws Exception;

    /**
     *
     * @param param drc build param
     * @return drc xml
     */
    String buildDbApplier(DrcBuildParam param) throws Exception;

    List<DbApplierDto> getMhaDbMessengers(String mhaName) throws Exception;

    String buildDbMessenger(DrcBuildBaseParam param) throws Exception;

    boolean isDbApplierConfigurable(String mhaName);
}
