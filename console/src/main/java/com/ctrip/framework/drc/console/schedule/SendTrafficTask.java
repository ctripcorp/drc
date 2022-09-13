package com.ctrip.framework.drc.console.schedule;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.framework.drc.core.service.statistics.traffic.*;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.xpipe.api.monitor.Task;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by jixinwang on 2022/9/9
 */
@Component
@Order(2)
public class SendTrafficTask extends AbstractLeaderAwareMonitor {

    private static final String SHA = "sha";
    private static final String SIN = "sin";
    private static final String FRA = "fra";

    private static final String FRA_AWS = "FRA-AWS";
    private static final String SHAXY = "SHAXY";
    private static final String SIN_AWS = "SIN-AWS";

    private static final String AWS_PROVIDER = "AWS";

    private Long currentTimeRoundToHour;

    private Map<String, DbTbl> dbMap = Maps.newHashMap();

    @Autowired
    private MonitorTableSourceProvider configService;

    @Autowired
    private DomainConfig domainConfig;

    @Autowired
    private DbTblDao dbTblDao;

    private OPSApiService opsApiService = ApiContainer.getOPSApiServiceImpl();

    private TrafficStatisticsService statisticsService = ApiContainer.getTrafficStatisticsService();

    private String accessToken;

    private String hickWallUrl;

    @Override
    public void initialize() {
        final int initialDelay = 60 * 5;
        final int period = 60 * 60;
        setInitialDelay(initialDelay);
        setPeriod(period);
        setTimeUnit(TimeUnit.SECONDS);
        this.accessToken = domainConfig.getCmsAccessToken();
        this.hickWallUrl = domainConfig.getTrafficFromHickWall();
    }

    @Override
    public void scheduledTask() throws Throwable {
        try {
            if (isRegionLeader) {
<<<<<<< HEAD
=======
                logger.info("[[task=sendTraffic]] is leader");
>>>>>>> b262150c2be63ec52a232c90dea5ca704153b169
                final String sendTrafficSwitch = configService.getSendTrafficSwitch();
                if ("on".equals(sendTrafficSwitch)) {
                    logger.info("[[task=sendTraffic] start");
                    DefaultTransactionMonitorHolder.getInstance().logTransaction("Schedule", "SendTraffic", new Task() {
                        @Override
                        public void go() throws Exception {
                            dbMap = getDbInfo();
                            sendTraffic();
                        }
                    });
                } else {
                    logger.warn("[[task=sendTraffic]]switch is off");
                }
            } else {
                logger.info("[[task=sendTraffic]]not a leader do nothing");
            }
        } catch (Throwable t) {
            logger.error("[[task=sendTraffic]] log error", t);
        }
    }

    private Map<String, DbTbl> getDbInfo() throws SQLException {
        Map<String, DbTbl> dbMap = Maps.newHashMap();
        DbTbl sample = new DbTbl();
        sample.setDeleted(0);
        List<DbTbl> dbs = dbTblDao.queryBy(sample);
        for (DbTbl db : dbs) {
            dbMap.put(db.getDbName(), db);
        }
        return dbMap;
    }

    private void sendTraffic() throws Exception {
        currentTimeRoundToHour = System.currentTimeMillis() / 1000 / (60 * 60) * (60 * 60);
        logger.info("[[task=sendTraffic]] current time round to hour: {}", currentTimeRoundToHour);

        HickWallTrafficContext shaToSinContext = getHickWallTrafficContext(SHA, SIN);
        List<HickWallTrafficEntity> shaToSin = opsApiService.getTrafficFromHickWall(shaToSinContext);
        sendToKafKa(shaToSin, AWS_PROVIDER, SIN_AWS, CostType.storage, 1);

        HickWallTrafficContext sinToShaContext = getHickWallTrafficContext(SIN, SHA);
        List<HickWallTrafficEntity> sinToSha = opsApiService.getTrafficFromHickWall(sinToShaContext);
        sendToKafKa(sinToSha, AWS_PROVIDER, SIN_AWS, CostType.storage, 1);
        sendToKafKa(sinToSha, AWS_PROVIDER, SIN_AWS, CostType.flow, 9);

        HickWallTrafficContext shaToFraContext = getHickWallTrafficContext(SHA, FRA);
        List<HickWallTrafficEntity> shaToFra = opsApiService.getTrafficFromHickWall(shaToFraContext);
        sendToKafKa(shaToFra, AWS_PROVIDER, FRA_AWS, CostType.storage, 1);

        HickWallTrafficContext fraToShaContext = getHickWallTrafficContext(FRA, SHA);
        List<HickWallTrafficEntity> fraToSha = opsApiService.getTrafficFromHickWall(fraToShaContext);
        sendToKafKa(fraToSha, AWS_PROVIDER, FRA_AWS, CostType.storage, 1);
        sendToKafKa(fraToSha, AWS_PROVIDER, FRA_AWS, CostType.flow, 9);
    }

    private HickWallTrafficContext getHickWallTrafficContext(String srcRegion, String dstRegion) {
        return new HickWallTrafficContext(srcRegion, dstRegion, currentTimeRoundToHour, hickWallUrl, accessToken);
    }

    private void sendToKafKa(List<HickWallTrafficEntity> costs, String provider, String region, CostType costType, int rate) {
        if (costs == null) {
            return;
        }
        for (HickWallTrafficEntity cost : costs) {
            String dbName = cost.getMetric().getDbName();
            if (dbName == null) {
                continue;
            }
            DbTbl db = dbMap.get(dbName);
            if (db == null) {
                continue;
            }
            String owner = db.getDbOwner();
            String buCode = db.getBuCode();

            KafKaTrafficMetric metric = new KafKaTrafficMetric();
            metric.setTimestamp(currentTimeRoundToHour);
            metric.setCloud_provider(provider);
            metric.setRegion(region);
            metric.setZone("");
            metric.setApp_name("drc");
            metric.setService_type("drc");
            metric.setApp_platform(costType.getName());
            metric.setApp_instance(dbName);
            metric.setOperation("RunInstances");
            metric.setShare_unit_type(costType.getName());
            List<Object> value = (List<Object>) (cost.getValues().get(0));
            metric.setShare_unit_amount(Float.parseFloat(value.get(1).toString()) * rate);
            metric.setCost_group(costType.getName());
            metric.setOwner(owner);
            metric.setBu_code(buCode);
            metric.setProduct_line_code("");

            try {
                statisticsService.send(metric);
            } catch (Exception e) {
<<<<<<< HEAD
                logger.info("[[task=sendTraffic]] send to kafka error: {}", metric, e);
=======
                logger.error("[[task=sendTraffic]] send to kafka error: {}", metric, e);
>>>>>>> b262150c2be63ec52a232c90dea5ca704153b169
            }

        }
    }
}
