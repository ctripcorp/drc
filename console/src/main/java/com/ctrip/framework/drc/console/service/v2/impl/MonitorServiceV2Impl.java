package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.aop.forward.PossibleRemote;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ForwardTypeEnum;
import com.ctrip.framework.drc.console.service.v2.MonitorServiceV2;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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


    @Override
    @PossibleRemote(path = "/api/drc/v2/monitor/mhaNames",forwardType = ForwardTypeEnum.TO_META_DB)
    public List<String> getMhaNamesToBeMonitored() throws SQLException  {
        List<String> mhaNamesToBeMonitored;
        // vpc
        Set<String> localConfigCloudDc = consoleConfig.getLocalConfigCloudDc();
        if (localConfigCloudDc.contains(consoleConfig.getRegion())) {
            mhaNamesToBeMonitored = consoleConfig.getLocalDcMhaNamesToBeMonitored();
            logger.info("get mha name to be monitored from local config: {}", JsonUtils.toJson(mhaNamesToBeMonitored));
        }
        // mhas which monitor switch on
        MhaTblV2 sample = new MhaTblV2();
        sample.setDeleted(BooleanEnum.FALSE.getCode());
        sample.setMonitorSwitch(1);
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryBy(sample);
        return mhaTblV2s.stream().map(MhaTblV2::getMhaName).collect(Collectors.toList());
    }
    
    
    
}
