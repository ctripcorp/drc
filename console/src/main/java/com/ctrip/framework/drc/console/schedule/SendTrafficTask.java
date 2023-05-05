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

    private static final String schemaVersion = "v1.1";

    private static final String SHA = "sha";
    private static final String SIN = "sin";
    private static final String FRA = "fra";
    private static final String ALI = "ali";

    private static final String FRA_AWS = "FRA-AWS";
    private static final String SHAXY = "SHA";
    private static final String SIN_AWS = "SIN-AWS";
    private static final String SHA_ALI = "SHA-ALI";

    private static final String AWS_PROVIDER = "aws";
    private static final String ALIYUN_PROVIDER = "aliyun";
    private static final String TRIP_PROVIDER = "trip";

    private static final String ACCOUNT_ID_AWS_PROD = "671660153913";
    private static final String ACCOUNT_ID_ALIYUN_PROD = "1963804511755329";
    private static final String ACCOUNT_ID_TRIP_PROD = "";

    private static final String ACCOUNT_NAME_AWS_PROD = "aws_prod_share";
    private static final String ACCOUNT_NAME_ALIYUN_PROD = "aliyun_prod_share";
    private static final String ACCOUNT_NAME_TRIP_PROD = "";

    private static final String appName = "drc";
    private static final String serviceTypeStorage = "drc-storage";
    private static final String serviceTypeFlow = "drc-flow";

    private static final ProviderContext AWS_PROVIDER_CONTEXT = new ProviderContext(AWS_PROVIDER, ACCOUNT_ID_AWS_PROD, ACCOUNT_NAME_AWS_PROD);
    private static final ProviderContext ALIYUN_PROVIDER_CONTEXT = new ProviderContext(ALIYUN_PROVIDER, ACCOUNT_ID_ALIYUN_PROD, ACCOUNT_NAME_ALIYUN_PROD);
    private static final ProviderContext TRIP_PROVIDER_CONTEXT = new ProviderContext(TRIP_PROVIDER, ACCOUNT_ID_TRIP_PROD, ACCOUNT_NAME_TRIP_PROD);

    private static final int batchSize = 100;

    private static final int initialDelay = 5 * 60;

    private static final int delay = 60 * 60;

    private Long currentTimeRoundToHour;

    private Map<String, DbTbl> dbMap = Maps.newHashMap();

    private Map<String, String> parentRelationGroups = Maps.newHashMap();

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
                                currentTimeRoundToHour = System.currentTimeMillis() / 1000 / (60 * 60) * (60 * 60);
                                dbMap = getDbInfo();
                                sendRelationCost();
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

    private void sendRelationCost() {
        parentRelationGroups = configService.getParentRelationGroups();
        Map<String, Set<String>> apps = configService.getRelationCostApps();
        for (Map.Entry<String, Set<String>> entry : apps.entrySet()) {
            String key = entry.getKey();
            Set<String> value = entry.getValue();
            sendRelationCostToKafka(key, value, AWS_PROVIDER_CONTEXT, SIN_AWS);
            sendRelationCostToKafka(key, value, AWS_PROVIDER_CONTEXT, FRA_AWS);
            sendRelationCostToKafka(key, value, ALIYUN_PROVIDER_CONTEXT, SHA_ALI);
            sendRelationCostToKafka(key, value, TRIP_PROVIDER_CONTEXT, SHAXY);
        }
    }

    private void sendRelationCostToKafka(String referedName, Set<String> referedInstances, ProviderContext providerContext, String region) {
        String costGroup = CostType.storage.getName();
        for (String instance : referedInstances) {
            RelationCostMetric metric = new RelationCostMetric();
            metric.setTimestamp(currentTimeRoundToHour);
            metric.setCloud_provider(providerContext.getCloud_provider());
            metric.setAccount_id(providerContext.getAccount_id());
            metric.setAccount_name(providerContext.getAccount_name());
            metric.setRegion(region);
            metric.setZone("");
            metric.setService_type(serviceTypeStorage);
            metric.setCost_group(costGroup);
            metric.setRefered_service_type(referedName);
            metric.setRefered_app_instance(instance);
            metric.setRelation_group(String.format("%s.%s", serviceTypeStorage, costGroup));
            String parentRelationGroup = parentRelationGroups.get(referedName);
            metric.setParent_relation_group(parentRelationGroup == null ? "" : parentRelationGroup);
            metric.set_schema_version(schemaVersion);

            try {
                statisticsService.send(metric);
            } catch (Exception e) {
                logger.error("[[task=sendTraffic]] send relation to kafka error: {}", metric, e);
            }
        }
    }


    private void sendTraffic() throws Exception {
        logger.info("[[task=sendTraffic]] current time round to hour: {}", currentTimeRoundToHour);

        // sin->sha
        HickWallTrafficContext sinToShaContext = getHickWallTrafficContext(SIN, SHA);
        List<HickWallTrafficEntity> sinToSha = opsApiService.getTrafficFromHickWall(sinToShaContext);
        sendToKafKa(sinToSha, AWS_PROVIDER_CONTEXT, SIN_AWS, serviceTypeStorage, CostType.storage, 1);
        sendToKafKa(sinToSha, AWS_PROVIDER_CONTEXT, SIN_AWS, serviceTypeFlow, CostType.flow, 9);
        sendToCat(sinToSha);
        sendToKafKa(sinToSha, TRIP_PROVIDER_CONTEXT, SHAXY, serviceTypeStorage, CostType.storage, 1);

        // fra->sha
        HickWallTrafficContext fraToShaContext = getHickWallTrafficContext(FRA, SHA);
        List<HickWallTrafficEntity> fraToSha = opsApiService.getTrafficFromHickWall(fraToShaContext);
        sendToKafKa(fraToSha, AWS_PROVIDER_CONTEXT, FRA_AWS, serviceTypeStorage, CostType.storage, 1);
        sendToKafKa(fraToSha, AWS_PROVIDER_CONTEXT, FRA_AWS, serviceTypeFlow, CostType.flow, 9);
        sendToCat(fraToSha);
        sendToKafKa(fraToSha, TRIP_PROVIDER_CONTEXT, SHAXY, serviceTypeStorage, CostType.storage, 1);

        // ali->sha
        HickWallTrafficContext aliToShaContext = getHickWallTrafficContext(ALI, SHA);
        List<HickWallTrafficEntity> aliToSha = opsApiService.getTrafficFromHickWall(aliToShaContext);
        sendToKafKa(aliToSha, ALIYUN_PROVIDER_CONTEXT, SHA_ALI, serviceTypeStorage, CostType.storage, 1);
        sendToCat(aliToSha);
        sendToKafKa(aliToSha, TRIP_PROVIDER_CONTEXT, SHAXY, serviceTypeStorage, CostType.storage, 1);

        // sha->sha
        HickWallTrafficContext shaToShaContext = getHickWallTrafficContext(SHA, SHA);
        List<HickWallTrafficEntity> shaToSha = opsApiService.getTrafficFromHickWall(shaToShaContext);
        sendToCat(shaToSha);
        sendToKafKa(shaToSha, TRIP_PROVIDER_CONTEXT, SHAXY, serviceTypeStorage, CostType.storage, 1);

        // sha->sin
        HickWallTrafficContext shaToSinContext = getHickWallTrafficContext(SHA, SIN);
        List<HickWallTrafficEntity> shaToSin = opsApiService.getTrafficFromHickWall(shaToSinContext);
        sendToKafKa(shaToSin, AWS_PROVIDER_CONTEXT, SIN_AWS, serviceTypeStorage, CostType.storage, 1);
        sendToCat(shaToSin);
        sendToKafKa(shaToSin, TRIP_PROVIDER_CONTEXT, SHAXY, serviceTypeStorage, CostType.storage, 1);

        // sha->fra
        HickWallTrafficContext shaToFraContext = getHickWallTrafficContext(SHA, FRA);
        List<HickWallTrafficEntity> shaToFra = opsApiService.getTrafficFromHickWall(shaToFraContext);
        sendToKafKa(shaToFra, AWS_PROVIDER_CONTEXT, FRA_AWS, serviceTypeStorage, CostType.storage, 1);
        sendToCat(shaToFra);
        sendToKafKa(shaToFra, TRIP_PROVIDER_CONTEXT, SHAXY, serviceTypeStorage, CostType.storage, 1);

        // sha->ali
        HickWallTrafficContext shaToAliContext = getHickWallTrafficContext(SHA, ALI);
        List<HickWallTrafficEntity> shaToAli = opsApiService.getTrafficFromHickWall(shaToAliContext);
        sendToKafKa(shaToAli, ALIYUN_PROVIDER_CONTEXT, SHA_ALI, serviceTypeStorage, CostType.storage, 1);
        sendToCat(shaToAli);
        sendToKafKa(shaToAli, TRIP_PROVIDER_CONTEXT, SHAXY, serviceTypeStorage, CostType.storage, 1);
    }

    private HickWallTrafficContext getHickWallTrafficContext(String srcRegion, String dstRegion) {
        return new HickWallTrafficContext(srcRegion, dstRegion, currentTimeRoundToHour, hickWallUrl, accessToken);
    }

    private void sendToKafKa(List<HickWallTrafficEntity> costs, ProviderContext providerContext, String region, String serviceType, CostType costType, int rate) {
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
            metric.setCloud_provider(providerContext.getCloud_provider());
            metric.setAccount_id(providerContext.getAccount_id());
            metric.setAccount_name(providerContext.getAccount_name());
            metric.setRegion(region);
            metric.setZone("");
            metric.setApp_name(appName);
            metric.setService_type(serviceType);
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
