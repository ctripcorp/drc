package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.AbstractMonitor;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.MhaServiceImpl;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-07
 */
@Component
public class PeriodicalUpdateMhasTask extends AbstractMonitor {

    private final Logger logger = LoggerFactory.getLogger("bizResourceLogger");

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private MhaServiceImpl mhaService;

    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;

    private DalUtils dalUtils = DalUtils.getInstance();

    // key: zoneId, val: dcId
    private Map<String, String> dcMap;

    private static final String SWITCH_STATUS_ON = "on";

    @Override
    public void initialize() {
        super.initialize();
        dcMap = defaultConsoleConfig.getDbaDcInfos();
    }

    @Override
    public void scheduledTask() {
        String updateClusterTblSwitch = monitorTableSourceProvider.getUpdateClusterTblSwitch();
        if(SWITCH_STATUS_ON.equalsIgnoreCase(updateClusterTblSwitch)) {
            try {
                List<MhaTbl> mhaTbls = dalUtils.getMhaTblDao().queryAll();
                Set<String> allMhaNamesFromMeta = Sets.newHashSet();
                mhaTbls.forEach(mhaTbl -> allMhaNamesFromMeta.add(mhaTbl.getMhaName()));

                List<Map<String, String>> allMhaMapsFromDba = mhaService.getAllClusterNames();

                List<MhaTbl> mhaTblList = getNewMhas(allMhaNamesFromMeta, allMhaMapsFromDba);

                dalUtils.getMhaTblDao().combinedInsert(mhaTblList);
            } catch (SQLException e) {
                logger.error("Fail to get all MhaTbls from metadb");
            }
        }
    }

    protected List<MhaTbl> getNewMhas(Set<String> allMhaNamesFromMeta, List<Map<String, String>> allMhaMapsFromDba) {
        List<MhaTbl> mhaTblList = Lists.newArrayList();
        for(Map<String, String> allClusterName : allMhaMapsFromDba) {
            String mhaName = allClusterName.get("cluster");
            String zoneId = allClusterName.get("zoneId");
            if(!dcMap.containsKey(zoneId)) {
                // set temp abbr dcId as zoneId
                dcMap.put(zoneId, zoneId);
                logger.info("{} not in dbaDcInfo", zoneId);
            }
            String dcName = dcMap.get(zoneId);
            if(null != mhaName && !allMhaNamesFromMeta.contains(mhaName)) {
                try {
                    Long dcId = dalUtils.updateOrCreateDc(dcName);
                    MhaTbl mhaPojo = dalUtils.createMhaPojo(mhaName, null, dcId);
                    mhaTblList.add(mhaPojo);
                } catch (SQLException e) {
                    logger.error("Fail to update mha: {}", mhaName, e);
                }
            }
        }
        return mhaTblList;
    }
}
