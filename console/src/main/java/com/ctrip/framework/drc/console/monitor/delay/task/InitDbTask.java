package com.ctrip.framework.drc.console.monitor.delay.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.service.impl.openapi.OpenService;
import com.ctrip.framework.drc.core.service.utils.Constants;
import com.ctrip.framework.drc.console.utils.JsonUtils;
import com.ctrip.framework.drc.console.vo.response.MhaResponseVo;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dbs;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-12
 * STEP 1: init the drcmointordb.delaymonitor table for periodically updating it
 */
@Component
public class InitDbTask {

    private final Logger logger = LoggerFactory.getLogger("delayMonitorLogger");

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @Autowired
    private OpenService openService;

    /**
     * change field name temporarily
     */
    public static final String INIT_INSERT_SQL = "REPLACE INTO `drcmonitordb`.`delaymonitor`(`id`, `src_ip`, `dest_ip`) VALUES(%s, '%s', '%s');";

    private static final String CHECK_EXIST_SQL = "SELECT * FROM `drcmonitordb`.`delaymonitor` WHERE `id`=%s AND `src_ip`='%s' AND `dest_ip`='%s';";

    private static final String CHECK_REDUNDANT_SQL = "SELECT * FROM `drcmonitordb`.`delaymonitor` WHERE `id`<>%s AND `src_ip`='%s';";

    private static final String DELETE_REDUNDANT_SQL = "DELETE FROM `drcmonitordb`.`delaymonitor` WHERE `id`<>%s AND `src_ip`='%s';";

    private static final String SWITCH_STATUS_ON = "on";

    private ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("InitDbTask");

    private static final int INITIAL_DELAY = 1;

    private static final int PERIOD = 5;

    @PostConstruct
    public void initdbTask() {
        scheduledExecutorService.scheduleWithFixedDelay(new InitDelayMonitorTblRunnable(), INITIAL_DELAY, PERIOD, TimeUnit.MINUTES);
    }

    public class InitDelayMonitorTblRunnable implements Runnable {

        @Override
        public void run() {
            try {
                final String initDelayMonitorRecordSwitch = monitorTableSourceProvider.getInitDelayMonitorRecordSwitch();
                logger.info("[[monitor=initdb]] init delay monitor record switch is: {}", initDelayMonitorRecordSwitch);
                if(SWITCH_STATUS_ON.equalsIgnoreCase(initDelayMonitorRecordSwitch)) {
                    try {
                        initDelayMonitorTbl();
                    } catch (Exception e) {
                        logger.error("[[monitor=initdb]] InitDbTask.initDelayMonitorTbl(): ", e);
                    }
                }
            } catch (Throwable t) {
                logger.error("[[monitor=initdb]] init db task error", t);
            }

        }

    }

    public void initDelayMonitorTbl() throws Exception {
        String currentDcName = dbClusterSourceProvider.getLocalDcName();
        Dc dc = dbClusterSourceProvider.getLocalDc();
        logger.info("[[monitor=initdb]] init: {}", currentDcName);

//        Set<String> grayMha = consoleConfig.getGrayMha();
//        String grayMhaSwitch = consoleConfig.getGrayMhaSwitch();
//        logger.info("[[monitor=initdb]] gray mhas are: {}", grayMha);
//        logger.info("[[monitor=initdb]] gray mha switch is: {}", grayMhaSwitch);

        if(null != dc) {
            logger.info("[[monitor=initdb]] init detail: {}", dc.toString());
            Map<String, DbCluster> dbClusters = dc.getDbClusters();

            Map<String, Long> mhaNameAndIdMap = getMhaNameAndIdMap(currentDcName);
            logger.info("[[monitor=initdb]] mha name and id map: {}", mhaNameAndIdMap);

            for(Map.Entry<String, DbCluster> entry : dbClusters.entrySet()) {
//                if(SWITCH_STATUS_ON.equalsIgnoreCase(grayMhaSwitch) && !isGrayMha(grayMha, entry.getValue())) {
//                    continue;
//                }

                try {
                    insertMhaDelayMonitorRecord(entry, mhaNameAndIdMap);
                } catch (Exception e) {
                    logger.error("[[monitor=initdb]] insert mha delay monitor record error, mha name is: {}, " +
                            "exception is: {}",  entry.getValue().getMhaName(), e.getMessage());
                }
            }
        }
    }

//    private boolean isGrayMha(Set<String> grayMha, DbCluster dbCluster) {
//        String mhaName = dbCluster.getMhaName();
//
//        if (grayMha.contains(mhaName)) {
//            logger.info("[[monitor=initdb]] current mha is in gray list, mha name is: {}", mhaName);
//            return true;
//        } else {
//            logger.info("[[monitor=initdb]] current mha is not in gray list, mha name is: {}", mhaName);
//            return false;
//        }
//    }

    public void insertMhaDelayMonitorRecord(Map.Entry<String, DbCluster> entry, Map<String, Long> mhaNameAndIdMap) throws Exception {
        String dbClusterId = entry.getKey();
        DbCluster dbCluster = entry.getValue();
        String mhaName = dbCluster.getMhaName();
        String currentDcName = dbClusterSourceProvider.getLocalDcName();

        Dbs dbs = dbCluster.getDbs();
        String monitorUser = dbs.getMonitorUser();
        String monitorPassword = dbs.getMonitorPassword();
        List<Db> dbList = dbs.getDbs();
        for(Db db : dbList) {
            if(db.isMaster()) {
                String ip = db.getIp();
                Integer port = db.getPort();
                Endpoint endpoint = new MySqlEndpoint(ip, port, monitorUser, monitorPassword, BooleanEnum.TRUE.isValue());
                WriteSqlOperatorWrapper writeSqlOperatorWrapper = new WriteSqlOperatorWrapper(endpoint);
                writeSqlOperatorWrapper.initialize();
                writeSqlOperatorWrapper.start();
                Long mhaId = mhaNameAndIdMap.get(mhaName);

                if (Objects.isNull(mhaId)) {
                    logger.warn("[[monitor=initdb]] can not get corresponding mha id, mha name is: {}", mhaName);
                    continue;
                }

                insertDelayMonitorRecord(mhaId, mhaName, currentDcName, writeSqlOperatorWrapper,
                        dbClusterId, endpoint);

                final String deleteRedundantDelayMonitorRecordSwitch = monitorTableSourceProvider.getDeleteRedundantDelayMonitorRecordSwitch();
                logger.info("[[monitor=initdb]] delete redundant delay monitor record switch is: {}", deleteRedundantDelayMonitorRecordSwitch);
                if (SWITCH_STATUS_ON.equalsIgnoreCase(deleteRedundantDelayMonitorRecordSwitch)) {
                    deleteRedundantDelayMonitorRecord(mhaId, currentDcName, writeSqlOperatorWrapper);
                }
            }
        }
    }

    private void insertDelayMonitorRecord(Long mhaId, String mhaName, String currentDcName,
                                          WriteSqlOperatorWrapper writeSqlOperatorWrapper,
                                          String dbClusterId, Endpoint endpoint) throws SQLException {
        String checkExistSql = String.format(CHECK_EXIST_SQL, mhaId, currentDcName, mhaName);
        logger.info("[[monitor=initdb]] check exist sql is: {}", checkExistSql);
        if(!isExist(checkExistSql, writeSqlOperatorWrapper)) {
            String sql = String.format(INIT_INSERT_SQL, mhaId, currentDcName, mhaName);
            logger.info("[[monitor=initdb]] init insert sql is: {}", sql);
            GeneralSingleExecution execution = new GeneralSingleExecution(sql);
            writeSqlOperatorWrapper.insert(execution);
            logger.info("[[monitor=initdb]] inited: {}-{}-{}:{}", currentDcName, dbClusterId, endpoint.getHost(), endpoint.getPort());
        }
    }

    private void deleteRedundantDelayMonitorRecord(Long mhaId, String dcName, WriteSqlOperatorWrapper writeSqlOperatorWrapper) throws SQLException {
        String checkRedundantSql = String.format(CHECK_REDUNDANT_SQL, mhaId, dcName);
        logger.info("[[monitor=initdb]] check redundant sql is: {}", checkRedundantSql);
        if(isExist(checkRedundantSql, writeSqlOperatorWrapper)) {
            String sql = String.format(DELETE_REDUNDANT_SQL, mhaId, dcName);
            logger.info("[[monitor=initdb]] delete redundant sql is: {}" ,sql);
            GeneralSingleExecution execution = new GeneralSingleExecution(sql);
            writeSqlOperatorWrapper.delete(execution);
        }
    }

    public Map<String, Long> getMhaNameAndIdMap(String dcName) throws SQLException {
        Map<String, Long> mhaNameAndIdMap = Maps.newHashMap();
        List<MhaTbl> mhaTbls = getMhasByDcName(dcName);
        mhaTbls.forEach(mhaTbl -> {
            mhaNameAndIdMap.put(mhaTbl.getMhaName(), mhaTbl.getId());
        });
        return mhaNameAndIdMap;
    }

    private List<MhaTbl> getMhasByDcName(String dcName) throws SQLException {
        Set<String> publicCloudDc = consoleConfig.getPublicCloudDc();
        List<MhaTbl> mhaTbls;

        if (publicCloudDc.contains(dcName.toLowerCase())) {
            mhaTbls = getRemoteMhas(dcName);
            logger.info("[[monitor=initdb]] get mha from remote: {}", JsonUtils.toJson(mhaTbls));
        } else {
            mhaTbls = getLocalMhas(dcName);
            logger.info("[[monitor=initdb]] get mha from local: {}", JsonUtils.toJson(mhaTbls));
        }

        return mhaTbls;
    }

    private List<MhaTbl> getRemoteMhas(String dcName) {
        Map<String, String> consoleDcInfos = consoleConfig.getConsoleDcInfos();

        if(consoleDcInfos.size() != 0) {
            for(Map.Entry<String, String> entry : consoleDcInfos.entrySet()) {
                if(!entry.getKey().equalsIgnoreCase(dcName)) {
                    try {
                        String uri = String.format("%s/api/drc/v1/meta/mhas?dcName={dcName}", entry.getValue());
                        Map<String, String> params = Maps.newHashMap();
                        params.put("dcName", dcName);
                        MhaResponseVo mhaResponseVo = openService.getMhas(uri, params);

                        if (Constants.zero.equals(mhaResponseVo.getStatus())) {
                            return mhaResponseVo.getData();
                        }
                    } catch (Throwable t) {
                        logger.warn("[[monitor=initdb]] fail get mhas from remote: {}", entry.getKey(), t);
                    }
                }
            }
        }
        return Lists.newArrayList();
    }

    private List<MhaTbl> getLocalMhas(String dcName) throws SQLException {
        return metaInfoService.getMhas(dcName);
    }

    private boolean isExist(String sql, WriteSqlOperatorWrapper writeSqlOperatorWrapper) throws SQLException {
        ReadResource readResource = null;
        try {
            GeneralSingleExecution execution = new GeneralSingleExecution(sql);
            readResource = writeSqlOperatorWrapper.select(execution);
            ResultSet rs = readResource.getResultSet();
            if (rs == null) {
                return false;
            }
            return rs.next();
        } finally {
            if (readResource != null) {
                readResource.close();
            }
        }
    }
}
