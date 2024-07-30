package com.ctrip.framework.drc.applier.resource.position;

import java.sql.SQLException;

public class TransactionTableRepeatedUpdateException extends SQLException {
    public TransactionTableRepeatedUpdateException(Throwable cause) {
        super(cause);
    }
}
