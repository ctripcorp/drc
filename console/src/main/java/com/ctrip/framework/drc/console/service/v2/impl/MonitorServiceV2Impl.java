package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.aop.forward.PossibleRemote;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ForwardTypeEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.service.v2.MonitorServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.vo.response.MhaNamesResponseVo;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_OFF;

/**
 * @ClassName MonitorServiceV2Impl
 * @Author haodongPan
 * @Date 2023/7/26 14:14
 * @Version: $
 */
@Service
public class MonitorServiceV2Impl implements MonitorServiceV2 {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired private MhaTblV2Dao mhaTblV2Dao;

    @Autowired private DefaultConsoleConfig consoleConfig;

    @Autowired private MetaProviderV2 metaProviderV2;


    @Override
    @PossibleRemote(path = "/api/drc/v2/monitor/mhaNames",forwardType = ForwardTypeEnum.TO_META_DB,responseType = MhaNamesResponseVo.class)
    public List<String> getMhaNamesToBeMonitored() throws SQLException  {
        // mhas which monitor switch on
        MhaTblV2 sample = new MhaTblV2();
        sample.setDeleted(BooleanEnum.FALSE.getCode());
        sample.setMonitorSwitch(1);
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryBy(sample);
        return mhaTblV2s.stream().map(MhaTblV2::getMhaName).collect(Collectors.toList());
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/monitor/dstMhaNames",forwardType = ForwardTypeEnum.TO_META_DB,responseType = MhaNamesResponseVo.class)
    public List<String> getDestMhaNamesToBeMonitored() throws SQLException {
        Set<String> mhaNamesToBeMonitored = Sets.newHashSet(this.getMhaNamesToBeMonitored());
        Drc drc = metaProviderV2.getDrc();
        if (drc == null) {
            logger.info("[getMonitorMetaInfo] return drc null");
            throw new RuntimeException("get drc fail");
        }
        return drc.getDcs().values().stream()
                .flatMap(dc -> dc.getDbClusters().values().stream())
                .filter(e -> e.getAppliers().stream().anyMatch(k -> mhaNamesToBeMonitored.contains(k.getTargetMhaName())))// src is monitored
                .map(DbCluster::getMhaName) // get dest mha name
                .distinct().collect(Collectors.toList());
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void switchMonitors(String mhaName, String status) throws SQLException {
        try {
            MhaTblV2 mhaTbl = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
            int monitorSwitch = status.equalsIgnoreCase(SWITCH_STATUS_OFF) ? BooleanEnum.FALSE.getCode() : BooleanEnum.TRUE.getCode();
            mhaTbl.setMonitorSwitch(monitorSwitch);
            mhaTblV2Dao.update(mhaTbl);
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.DAO_TBL_EXCEPTION, e);
        }
    }

}
