package com.ctrip.framework.drc.monitor;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.monitor.config.ConfigService;
import com.ctrip.framework.drc.monitor.function.cases.manager.BilateralPairCaseManager;
import com.ctrip.framework.drc.monitor.function.cases.manager.DalPairCaseManager;
import com.ctrip.framework.drc.monitor.function.cases.manager.PairCaseManager;
import com.ctrip.framework.drc.monitor.function.cases.manager.suit.UcsUnitRouteCase;
import com.ctrip.framework.drc.monitor.function.cases.manager.suit.UniLateralInsertCase;
import com.ctrip.framework.drc.monitor.function.cases.manager.suit.UniLateralTruncateCase;
import com.ctrip.xpipe.api.monitor.Task;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by mingdongli
 * 2019/10/9 下午5:35.
 */
public class TripDrcMonitorModule extends DrcMonitorModule {

    private ScheduledExecutorService bilateralScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("bilateral-scheduledExecutorService");

    private PairCaseManager dalPairCaseManager = new DalPairCaseManager();

    private PairCaseManager bilateralPairCaseManager = new BilateralPairCaseManager();

    public TripDrcMonitorModule(int srcPort, int dstPort) {
        super(srcPort, dstPort);
    }

    public TripDrcMonitorModule(int srcPort, int dstPort, String password) {
        super(srcPort, dstPort, password);
    }

    public TripDrcMonitorModule(String srcIp, int srcPort, String dstIp, int dstPort, String user, String password) {
        super(srcIp, srcPort, dstIp, dstPort, user, password);
    }

    @Override
    protected void doStart() throws Exception{
        super.doStart();

        dalScheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    DefaultTransactionMonitorHolder.getInstance().logTransaction("Monitor", "Dal", new Task() {
                        @Override
                        public void go() throws Exception {
                            final boolean finalAutoDal = ConfigService.getInstance().getAutoDalSwitch();
                            if (finalAutoDal) {
                                dal();
                            }
                        }
                    });
                } catch (Throwable t) {
                    logger.error("dal error", t);
                }
            }
        }, 10, 100, TimeUnit.MILLISECONDS);


        bilateralScheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    DefaultTransactionMonitorHolder.getInstance().logTransaction("Monitor", "bi", new Task() {
                        @Override
                        public void go() throws Exception {
                            final boolean finalAutoBi = ConfigService.getInstance().getAutoBiSwitch();
                            if (finalAutoBi) {
                                bi();
                            }
                        }
                    });
                } catch (Throwable t) {
                    logger.error("bi error", t);
                }
            }
        }, 10, ConfigService.getInstance().getBiLateralSchedulePeriod(), TimeUnit.SECONDS);


    }

    @Override
    protected void addPairCase() {
        super.addPairCase();
        if (ConfigService.getInstance().getUnilateralTruncateSwitch()) {
            unilateralPairCaseManager.addPairCase(new UniLateralInsertCase());
            unilateralPairCaseManager.addPairCase(new UniLateralTruncateCase());
        }

        if(ConfigService.getInstance().getUcsUnitRouteSwitch()) {
            bilateralPairCaseManager.addPairCase(new UcsUnitRouteCase());
        }

    }

    protected void dal() {
        dalPairCaseManager.test(sourceSqlOperator, reverseSourceSqlOperator);
    }

    protected void bi() {
        if(ConfigService.getInstance().getUnilateralTruncateSwitch()) {
            UniLateralTruncateCase uniLateralTruncateCase = new UniLateralTruncateCase();
            uniLateralTruncateCase.doWrite(sourceSqlOperator, reverseSourceSqlOperator);
        }

        bilateralPairCaseManager.test(sourceSqlOperator, reverseSourceSqlOperator);
    }

}
