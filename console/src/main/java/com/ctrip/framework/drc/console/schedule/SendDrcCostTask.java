package com.ctrip.framework.drc.console.schedule;

import com.ctrip.framework.drc.console.cost.CostType;
import com.ctrip.framework.drc.console.cost.DrcCostKafkaSender;
import com.ctrip.framework.drc.console.cost.DrcCostMetric;
import com.ctrip.framework.drc.console.cost.FlowEntity;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.openapi.OpenService;
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
public class SendDrcCostTask extends AbstractLeaderAwareMonitor {

    private static final String SHA = "sha";
    private static final String SIN = "sin";
    private static final String FRA = "fra";

    private static final String FRA_AWS = "FRA-AWS";
    private static final String SHAXY = "SHAXY";
    private static final String SIN_AWS = "SIN-AWS";

    private static final String AWS_PROVIDER = "AWS";

    private Long currentTimeRoundToHour;

    private Map<String, DbTbl> dbMap = Maps.newConcurrentMap();

    @Autowired
    private OpenService openService;

    private DrcCostKafkaSender sender;

    @Autowired
    private MonitorTableSourceProvider configService;

    @Autowired
    private DbTblDao dbTblDao;


    @Override
    public void initialize() {
        final int initialDelay = 5;
        final int period = 60 * 60 * 24;
        setInitialDelay(initialDelay);
        setPeriod(period);
        setTimeUnit(TimeUnit.SECONDS);
    }

    @Override
    public void scheduledTask() throws Exception {
        sender = new DrcCostKafkaSender();
        sender.init();
        getDbInfo();
        sendDrcCost();
        try {
            final String sendFlowCostSwitch = configService.getSendFlowCostSwitch();
            if ("on".equals(sendFlowCostSwitch)) {
                logger.info("[[task=sendFlowCostSwitch] start");
                DefaultTransactionMonitorHolder.getInstance().logTransaction("Schedule", "SendFlowCost", new Task() {
                    @Override
                    public void go() throws Exception {
                        getDbInfo();
                        sendDrcCost();
                    }
                });

            } else {
                logger.warn("[[task=sendFlowCostSwitch]]switch is off");
            }

        } catch (Throwable t) {
            logger.error("[[task=sendFlowCostSwitch]] log error", t);
        }
    }

    private void getDbInfo() throws SQLException {
        DbTbl sample = new DbTbl();
        sample.setDeleted(0);
        List<DbTbl> dbs = dbTblDao.queryBy(sample);
        for (DbTbl db : dbs) {
            dbMap.put(db.getDbName(), db);
        }
    }

    private void sendDrcCost() throws Exception {
        currentTimeRoundToHour = System.currentTimeMillis() / 1000 / (60 * 60) * (60 * 60);
        logger.info("[[task=sendFlowCostSwitch]] current time round to hour: {}", currentTimeRoundToHour);

        List<FlowEntity> shaToSin = openService.getFlowCostFromHickWall(SHA, SIN, currentTimeRoundToHour);
        sendMachineCost(shaToSin, AWS_PROVIDER, SIN_AWS);

        List<FlowEntity> sinToSha = openService.getFlowCostFromHickWall(SIN, SHA, currentTimeRoundToHour);
        sendMachineCost(sinToSha, AWS_PROVIDER, SIN_AWS);
        sendFlowCost(sinToSha, AWS_PROVIDER, SIN_AWS);


        List<FlowEntity> shaToFra = openService.getFlowCostFromHickWall(SHA, FRA, currentTimeRoundToHour);
        sendMachineCost(shaToFra, AWS_PROVIDER, FRA_AWS);

        List<FlowEntity> fraToSha = openService.getFlowCostFromHickWall(FRA, SHA, currentTimeRoundToHour);
        sendMachineCost(fraToSha, AWS_PROVIDER, FRA_AWS);
        sendFlowCost(fraToSha, AWS_PROVIDER, FRA_AWS);

    }

    private void sendMachineCost(List<FlowEntity> cost, String provider, String region) {
        sendToKafKa(cost, provider, region, CostType.storage, 1);
    }

    private void sendFlowCost(List<FlowEntity> cost, String provider, String region) {
        sendToKafKa(cost, provider, region, CostType.flow, 9);
    }

    private void sendToKafKa(List<FlowEntity> costs, String provider, String region, CostType costType, int rate) {
        if (costs == null) {
            return;
        }
        for (FlowEntity cost : costs) {
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

            DrcCostMetric drcCostMetric = new DrcCostMetric();
            drcCostMetric.setTimestamp(currentTimeRoundToHour);
            drcCostMetric.setCloud_provider(provider);
            drcCostMetric.setRegion(region);
            drcCostMetric.setZone("");
            drcCostMetric.setApp_name("drc");
            drcCostMetric.setService_type("drc");
            drcCostMetric.setApp_platform(costType.getName());
            drcCostMetric.setApp_instance(dbName);
            drcCostMetric.setOperation("RunInstances");
            drcCostMetric.setShare_unit_type(costType.getName());
            List<Object> value = (List<Object>) (cost.getValues().get(0));
            drcCostMetric.setShare_unit_amount(Float.parseFloat(value.get(1).toString()) * rate);
            drcCostMetric.setCost_group(costType.getName());
            drcCostMetric.setOwner(owner);
            drcCostMetric.setBu_code(buCode);
            drcCostMetric.setProduct_line_code("");

            sender.send(drcCostMetric);
        }
    }
}
