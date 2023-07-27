package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.param.v2.DrcMhaBuildParam;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Created by dengquanliang
 * 2023/7/27 15:43
 */
@Service
public class DrcBuildServiceV2Impl implements DrcBuildServiceV2 {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void buildMha(DrcMhaBuildParam param) throws Exception {
        checkDrcMhaBuildParam(param);

    }

    private MhaTblV2(String mhaName, )

    private void checkDrcMhaBuildParam(DrcMhaBuildParam param) {
        PreconditionUtils.checkNotNull(param);
        PreconditionUtils.checkString(param.getSrcMhaName(), "srcMhaName requires not empty!");
        PreconditionUtils.checkString(param.getDstMhaName(), "dstMhaName requires not empty!");
        PreconditionUtils.checkId(param.getBuId(), "buId requires not null!");
        PreconditionUtils.checkId(param.getSrcDcId(), "srcDcId requires not null!");
        PreconditionUtils.checkId(param.getDstDcId(), "dstDcId requires not null!");

    }
}
