package com.ctrip.framework.drc.replicator.impl.oubound;

import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;

/**
 * @author yongnian
 */
public class ReplicatorException extends RuntimeException {
    private OutboundLogEventContext context;
    private final ResultCode resultCode;

    public ResultCode getResultCode() {
        return resultCode;
    }

    public ReplicatorException(ResultCode resultCode) {
        super(resultCode.getMessage());
        this.resultCode = resultCode;
    }

    public ReplicatorException(ResultCode resultCode, String msg) {
        super(resultCode.getMessage() + ": " + msg);
        this.resultCode = resultCode;
    }

    public OutboundLogEventContext getContext() {
        return context;
    }

    public ReplicatorException(ResultCode resultCode, OutboundLogEventContext value) {
        super(resultCode.getMessage());
        this.resultCode = resultCode;
        this.context = value;
    }
}
