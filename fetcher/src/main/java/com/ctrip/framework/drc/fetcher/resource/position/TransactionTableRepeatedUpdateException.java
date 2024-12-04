package com.ctrip.framework.drc.fetcher.resource.position;

import java.sql.SQLException;

public class TransactionTableRepeatedUpdateException extends SQLException {
    public TransactionTableRepeatedUpdateException(Throwable cause) {
        super(cause);
    }
}
