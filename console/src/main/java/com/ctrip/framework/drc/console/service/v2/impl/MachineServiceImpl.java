package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.service.v2.MachineService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/7/27 15:43
 */
@Service
public class MachineServiceImpl implements MachineService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MhaTblV2Dao mhaTblDao;
    @Autowired
    private MachineTblDao machineTblDao;

    @Override
    public Endpoint getMasterEndpoint(String mha) {
        try {
            MhaTblV2 mhaTblV2 = mhaTblDao.queryByMhaName(mha, 0);
            List<MachineTbl> machineTbls = machineTblDao.queryByMhaId(mhaTblV2.getId(), 0);
            return getMaster(mhaTblV2, machineTbls);
        } catch (SQLException e) {
            logger.error("getDrcReplicationConfig sql exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    public Endpoint getMaster(MhaTblV2 mhaTblV2, List<MachineTbl> machineInfo) {
        for (MachineTbl machineTbl : machineInfo) {
            if (machineTbl.getMaster().equals(BooleanEnum.FALSE.getCode())) {
                return new MySqlEndpoint(machineTbl.getIp(), machineTbl.getPort(), mhaTblV2.getMonitorUser(), mhaTblV2.getMonitorPassword(), BooleanEnum.TRUE.isValue());
            }
        }
        return null;
    }
}
