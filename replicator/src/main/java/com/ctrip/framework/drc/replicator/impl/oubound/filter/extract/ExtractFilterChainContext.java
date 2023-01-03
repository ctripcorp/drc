package com.ctrip.framework.drc.replicator.impl.oubound.filter.extract;

import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundFilterChainContext;

/**
 * Created by jixinwang on 2022/12/29
 */
public class ExtractFilterChainContext {

    private DataMediaConfig dataMediaConfig;

    private OutboundMonitorReport outboundMonitorReport;

    public ExtractFilterChainContext(DataMediaConfig dataMediaConfig, OutboundMonitorReport outboundMonitorReport) {
        this.dataMediaConfig = dataMediaConfig;
        this.outboundMonitorReport = outboundMonitorReport;
    }

    public DataMediaConfig getDataMediaConfig() {
        return dataMediaConfig;
    }

    public void setDataMediaConfig(DataMediaConfig dataMediaConfig) {
        this.dataMediaConfig = dataMediaConfig;
    }

    public OutboundMonitorReport getOutboundMonitorReport() {
        return outboundMonitorReport;
    }

    public void setOutboundMonitorReport(OutboundMonitorReport outboundMonitorReport) {
        this.outboundMonitorReport = outboundMonitorReport;
    }

    public boolean shouldFilterRows() {
        if (dataMediaConfig == null) {
            return false;
        }
        return dataMediaConfig.shouldFilterRows();
    }

    public boolean shouldFilterColumns() {
        if (dataMediaConfig == null) {
            return false;
        }
        return dataMediaConfig.shouldFilterColumns();
    }

    public static ExtractFilterChainContext from(OutboundFilterChainContext outboundFilterChainContext) {
        return new ExtractFilterChainContext(outboundFilterChainContext.getDataMediaConfig(),
                outboundFilterChainContext.getOutboundMonitorReport());
    }
}
