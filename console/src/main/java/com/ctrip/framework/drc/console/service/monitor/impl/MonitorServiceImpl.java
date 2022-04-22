package com.ctrip.framework.drc.console.service.monitor.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.GroupMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.service.impl.openapi.OpenService;
import com.ctrip.framework.drc.console.service.monitor.MonitorService;
import com.ctrip.framework.drc.core.service.utils.Constants;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.JsonUtils;
import com.ctrip.framework.drc.console.vo.response.MhaNamesResponseVo;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_OFF;

/**
 * Created by jixinwang on 2021/8/2
 */
@Service
public class MonitorServiceImpl implements MonitorService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DbClusterSourceProvider sourceProvider;

    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @Autowired
    private OpenService openService;

    private DalUtils dalUtils = DalUtils.getInstance();

    @Override
    public void switchMonitors(List<Long> mhaGroupIds, String status) throws SQLException {
        int monitorSwitch = status.equalsIgnoreCase(SWITCH_STATUS_OFF) ? BooleanEnum.FALSE.getCode() : BooleanEnum.TRUE.getCode();
        List<MhaGroupTbl> mhaGroupTbls = Lists.newArrayList();
        mhaGroupIds.forEach(id -> {
            MhaGroupTbl mhaGroupTbl = new MhaGroupTbl();
            mhaGroupTbl.setId(id);
            mhaGroupTbl.setMonitorSwitch(monitorSwitch);
            mhaGroupTbls.add(mhaGroupTbl);
        });
        dalUtils.getMhaGroupTblDao().batchUpdate(mhaGroupTbls);
        // TODO 
        // this.defaultCurrentMetaManager.notify(mysql)
        // 其他 不用mysqlObserver 的monitor
    }

    @Override
    public List<String> queryMhaNamesToBeMonitored() throws SQLException {
        List<Long> mhaIdsTobeMonitored = queryMhaIdsToBeMonitored();
        MhaTbl mhaTbl = new MhaTbl();
        mhaTbl.setDeleted(BooleanEnum.FALSE.getCode());
        List<MhaTbl> mhaTbls = dalUtils.getMhaTblDao().queryBy(mhaTbl);
        return mhaTbls.stream().filter(p -> mhaIdsTobeMonitored.contains(p.getId()))
                .map(MhaTbl::getMhaName).collect(Collectors.toList());
    }

    @Override
    public List<Long> queryMhaIdsToBeMonitored() throws SQLException {
        MhaGroupTbl mhaGroupTbl = new MhaGroupTbl();
        mhaGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());
        mhaGroupTbl.setMonitorSwitch(BooleanEnum.TRUE.getCode());
        List<MhaGroupTbl> mhaGroupTbls = dalUtils.getMhaGroupTblDao().queryBy(mhaGroupTbl);
        List<Long> mhaGroupIdsTobeMonitored = mhaGroupTbls.stream().map(MhaGroupTbl::getId).collect(Collectors.toList());

        GroupMappingTbl groupMappingTbl = new GroupMappingTbl();
        groupMappingTbl.setDeleted(BooleanEnum.FALSE.getCode());
        List<GroupMappingTbl> groupMappingTbls = dalUtils.getGroupMappingTblDao().queryBy(groupMappingTbl);
        List<Long> mhaIdsTobeMonitored = groupMappingTbls.stream().filter(p -> mhaGroupIdsTobeMonitored
                .contains(p.getMhaGroupId())).map(GroupMappingTbl::getMhaId).collect(Collectors.toList());
        
        return mhaIdsTobeMonitored;
    }

    @Override
    public List<String> getMhaNamesToBeMonitored() throws SQLException {
        String currentDcName = sourceProvider.getLocalDcName();
        Set<String> publicCloudDc = consoleConfig.getPublicCloudDc();
        List<String> mhaNamesToBeMonitored;

        if (publicCloudDc.contains(currentDcName)) {
            Set<String> localConfigCloudDc = consoleConfig.getLocalConfigCloudDc();
            if (localConfigCloudDc.contains(currentDcName)) {
                mhaNamesToBeMonitored = consoleConfig.getLocalDcMhaNamesToBeMonitored();
                logger.info("get mha name to be monitored from local config: {}", JsonUtils.toJson(mhaNamesToBeMonitored));
            } else {
                mhaNamesToBeMonitored = getRemoteMhaNamesToBeMonitored(currentDcName);
                logger.info("get mha name to be monitored from remote: {}", JsonUtils.toJson(mhaNamesToBeMonitored));
            }
        } else {
            mhaNamesToBeMonitored = getLocalMhaNamesToBeMonitored();
            logger.info("get mha name to be monitored from local: {}", JsonUtils.toJson(mhaNamesToBeMonitored));
        }
        
        return mhaNamesToBeMonitored;
    }

    private List<String> getRemoteMhaNamesToBeMonitored(String currentDcName) throws IllegalStateException {
        Map<String, String> consoleDcInfos = consoleConfig.getConsoleDcInfos();

        if(consoleDcInfos.size() != 0) {
            for(Map.Entry<String, String> entry : consoleDcInfos.entrySet()) {
                if(!entry.getKey().equalsIgnoreCase(currentDcName)) {
                    String uri = String.format("%s/api/drc/v1/monitor/switches/on", entry.getValue());
                    MhaNamesResponseVo mhaNamesResponseVo = openService.getMhaNamesToBeMonitored(uri);

                    if (Constants.zero.equals(mhaNamesResponseVo.getStatus())) {
                        return mhaNamesResponseVo.getData();
                    }
                }
            }
        }
        logger.info("can not get remote mha names to be monitored");
        throw new IllegalStateException("get remote mha names to be monitored exception");
    }

    private List<String> getLocalMhaNamesToBeMonitored() throws SQLException {
        return queryMhaNamesToBeMonitored();
    }
}
