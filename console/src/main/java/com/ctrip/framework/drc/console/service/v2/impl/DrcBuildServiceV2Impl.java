package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.param.v2.DrcMhaBuildParam;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by dengquanliang
 * 2023/7/27 15:43
 */
@Service
public class DrcBuildServiceV2Impl implements DrcBuildServiceV2 {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    private static final String CLUSTER_NAME_SUFFIX = "_dalcluster";

    @Override
    public void buildMha(DrcMhaBuildParam param) throws Exception {
        checkDrcMhaBuildParam(param);
        String clusterName = param.getDstMhaName() + CLUSTER_NAME_SUFFIX;
        MhaTblV2 srcMha = buildMhaTbl(param.getSrcMhaName(), param.getSrcDcId(), param.getBuId(), clusterName);
        MhaTblV2 dstMha = buildMhaTbl(param.getDstMhaName(), param.getDstDcId(), param.getBuId(), clusterName);
    }

    private MhaTblV2 buildMhaTbl(String mhaName, long dcId, long buId, String clusterName) {
        MhaTblV2 mhaTblV2 = new MhaTblV2();
        mhaTblV2.setMhaName(mhaName);
        mhaTblV2.setDcId(dcId);
        mhaTblV2.setApplyMode(ApplyMode.transaction_table.getType());
        mhaTblV2.setMonitorSwitch(BooleanEnum.FALSE.getCode());
        mhaTblV2.setBuId(buId);
        mhaTblV2.setClusterName(clusterName);
        mhaTblV2.setAppId(-1L);
        mhaTblV2.setDeleted(BooleanEnum.FALSE.getCode());

        mhaTblV2.setReadUser(monitorTableSourceProvider.getReadUserVal());
        mhaTblV2.setReadPassword(monitorTableSourceProvider.getReadPasswordVal());
        mhaTblV2.setWriteUser(monitorTableSourceProvider.getWriteUserVal());
        mhaTblV2.setWritePassword(monitorTableSourceProvider.getWritePasswordVal());
        mhaTblV2.setMonitorUser(monitorTableSourceProvider.getMonitorUserVal());
        mhaTblV2.setMonitorPassword(monitorTableSourceProvider.getMonitorPasswordVal());

        return mhaTblV2;
    }

    private void checkDrcMhaBuildParam(DrcMhaBuildParam param) {
        PreconditionUtils.checkNotNull(param);
        PreconditionUtils.checkString(param.getSrcMhaName(), "srcMhaName requires not empty!");
        PreconditionUtils.checkString(param.getDstMhaName(), "dstMhaName requires not empty!");
        PreconditionUtils.checkId(param.getBuId(), "buId requires not null!");
        PreconditionUtils.checkId(param.getSrcDcId(), "srcDcId requires not null!");
        PreconditionUtils.checkId(param.getDstDcId(), "dstDcId requires not null!");

    }
}
