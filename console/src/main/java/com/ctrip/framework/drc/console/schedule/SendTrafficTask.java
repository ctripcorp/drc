package com.ctrip.framework.drc.console.schedule;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.framework.drc.core.service.statistics.traffic.*;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.xpipe.api.monitor.Task;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by jixinwang on 2022/9/9
 */
@Component
@Order(2)
public class SendTrafficTask extends AbstractLeaderAwareMonitor {

    private final Logger trafficLogger = LoggerFactory.getLogger("TRAFFIC");

    private static final String schemaVersion = "v1.0";

    private static final String SHA = "sha";
    private static final String SIN = "sin";
    private static final String FRA = "fra";
    private static final String ALI = "ali";

    private static final String FRA_AWS = "FRA-AWS";
    private static final String SHAXY = "SHAXY";
    private static final String SIN_AWS = "SIN-AWS";
    private static final String SHA_ALI = "SHA-ALI";

    private static final String AWS_PROVIDER = "aws";
    private static final String ALIYUN_PROVIDER = "aliyun";
    private static final String TRIP_PROVIDER = "trip";

    private static final int batchSize = 100;

    private static final int initialDelay = 5 * 60;

    private static final int delay = 60 * 60;

    private Long currentTimeRoundToHour;

    private Map<String, DbTbl> dbMap = Maps.newHashMap();

    @Autowired
    private MonitorTableSourceProvider configService;

    @Autowired
    private DomainConfig domainConfig;

    @Autowired
    private DbTblDao dbTblDao;

    public ScheduledExecutorService timer = ThreadUtils.newSingleThreadScheduledExecutor("Send-Traffic-Task");

    private OPSApiService opsApiService = ApiContainer.getOPSApiServiceImpl();

    private TrafficStatisticsService statisticsService = ApiContainer.getTrafficStatisticsService();

    private String accessToken;

    private String hickWallUrl;

    private Set<String> dbsSended = Sets.newHashSet();

    @Override
    public void initialize() {
        this.accessToken = domainConfig.getCmsAccessToken();
        this.hickWallUrl = domainConfig.getTrafficFromHickWall();
    }

    @PostConstruct
    @Override
    public void start() {
        initialize();
        startSendTrafficTask();
    }

    private void startSendTrafficTask() {
        timer.scheduleAtFixedRate(() -> {
            try {
                if (isRegionLeader) {
                    logger.info("[[task=sendTraffic]] is leader");
                    final String sendTrafficSwitch = configService.getSendTrafficSwitch();
                    if ("on".equals(sendTrafficSwitch)) {
                        logger.info("[[task=sendTraffic] start");
                        DefaultTransactionMonitorHolder.getInstance().logTransaction("Schedule", "SendTraffic", new Task() {
                            @Override
                            public void go() throws Exception {
                                dbMap = getDbInfo();
                                sendTraffic();
                                updateSendTime();
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
        }, initialDelay, delay, TimeUnit.SECONDS);
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

        // sin->sha
        HickWallTrafficContext sinToShaContext = getHickWallTrafficContext(SIN, SHA);
        List<HickWallTrafficEntity> sinToSha = opsApiService.getTrafficFromHickWall(sinToShaContext);
        sendToKafKa(sinToSha, AWS_PROVIDER, SIN_AWS, CostType.storage, 1);
        sendToKafKa(sinToSha, AWS_PROVIDER, SIN_AWS, CostType.flow, 9);
        sendToCat(sinToSha);

        // fra->sha
        HickWallTrafficContext fraToShaContext = getHickWallTrafficContext(FRA, SHA);
        List<HickWallTrafficEntity> fraToSha = opsApiService.getTrafficFromHickWall(fraToShaContext);
        sendToKafKa(fraToSha, AWS_PROVIDER, FRA_AWS, CostType.storage, 1);
        sendToKafKa(fraToSha, AWS_PROVIDER, FRA_AWS, CostType.flow, 9);
        sendToCat(fraToSha);

        // ali->sha
        HickWallTrafficContext aliToShaContext = getHickWallTrafficContext(ALI, SHA);
        List<HickWallTrafficEntity> aliToSha = opsApiService.getTrafficFromHickWall(aliToShaContext);
        sendToKafKa(aliToSha, ALIYUN_PROVIDER, SHA_ALI, CostType.storage, 1);
        sendToCat(aliToSha);

        // sha->sha
        HickWallTrafficContext shaToShaContext = getHickWallTrafficContext(SHA, SHA);
        List<HickWallTrafficEntity> shaToSha = opsApiService.getTrafficFromHickWall(shaToShaContext);
        sendToKafKa(shaToSha, TRIP_PROVIDER, SHAXY, CostType.storage, 1);
        sendToCat(shaToSha);

        // sha->sin
        HickWallTrafficContext shaToSinContext = getHickWallTrafficContext(SHA, SIN);
        List<HickWallTrafficEntity> shaToSin = opsApiService.getTrafficFromHickWall(shaToSinContext);
        sendToKafKa(shaToSin, AWS_PROVIDER, SIN_AWS, CostType.storage, 1);
        sendToCat(shaToSin);

        // sha->fra
        HickWallTrafficContext shaToFraContext = getHickWallTrafficContext(SHA, FRA);
        List<HickWallTrafficEntity> shaToFra = opsApiService.getTrafficFromHickWall(shaToFraContext);
        sendToKafKa(shaToFra, AWS_PROVIDER, FRA_AWS, CostType.storage, 1);
        sendToCat(shaToFra);

        // sha->ali
        HickWallTrafficContext shaToAliContext = getHickWallTrafficContext(SHA, ALI);
        List<HickWallTrafficEntity> shaToAli = opsApiService.getTrafficFromHickWall(shaToAliContext);
        sendToKafKa(shaToAli, ALIYUN_PROVIDER, SHA_ALI, CostType.storage, 1);
        sendToCat(shaToAli);
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

            if (currentTimeRoundToHour.equals(db.getTrafficSendLastTime())) {
                trafficLogger.warn("[cost] already send in:{} for db: {}, region: {}, type: {}", currentTimeRoundToHour, dbName, region, costType.getName());
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
            metric.setShare_unit_type(costType.getName());
            List<Object> value = (List<Object>) (cost.getValues().get(0));
            metric.setShare_unit_amount(Float.parseFloat(value.get(1).toString()) * rate);
            metric.setCost_group(costType.getName());
            metric.setOwner(owner);
            metric.setBu_code(buCode);
            metric.setProduct_line_code("");
            metric.set_schema_version(schemaVersion);

            try {
                statisticsService.send(metric);
                dbsSended.add(dbName);
            } catch (Exception e) {
                logger.error("[[task=sendTraffic]] send to kafka error: {}", metric, e);
            }
        }
    }

    private void sendToCat(List<HickWallTrafficEntity> costs) {
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

            if (currentTimeRoundToHour.equals(db.getTrafficSendLastTime())) {
                trafficLogger.warn("[cost] already send in:{} for db: {}", currentTimeRoundToHour, dbName);
                continue;
            }

            String buName = db.getBuName();

            CatTrafficMetric metric = new CatTrafficMetric();
            metric.setDbName(dbName);
            metric.setBuName(buName);
            List<Object> value = (List<Object>) (cost.getValues().get(0));
            metric.setCount("1");
            metric.setSize(Long.parseLong(value.get(1).toString()));
            statisticsService.send(metric);
            dbsSended.add(dbName);
        }
    }

    private void updateSendTime() throws SQLException {
        List<String> dbNames = Lists.newArrayList();
        for (String dbName : dbsSended) {
            dbNames.add(dbName);
            if (dbNames.size() == batchSize) {
                batchUpdateTime(dbNames);
                dbNames = Lists.newArrayList();
            }
        }
        if (dbNames.size() != batchSize) {
            batchUpdateTime(dbNames);
        }
    }

    private void batchUpdateTime(List<String> dbNames) throws SQLException {
        List<DbTbl> toUpdateDbs = Lists.newArrayList();
        for (String dbName : dbNames) {
            Long id = dbMap.get(dbName).getId();
            DbTbl dbTbl = new DbTbl();
            dbTbl.setId(id);
            dbTbl.setTrafficSendLastTime(currentTimeRoundToHour);
            toUpdateDbs.add(dbTbl);
        }
        dbTblDao.batchUpdate(toUpdateDbs);
    }
}
