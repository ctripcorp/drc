//package com.ctrip.framework.drc.fetcher.resource.context;
//
//import com.ctrip.framework.drc.fetcher.resource.position.TransactionTable;
//import com.ctrip.framework.drc.fetcher.system.InstanceResource;
//import com.ctrip.framework.drc.fetcher.system.Resource;
//
//import java.sql.Connection;
//
///**
// * Created by shiruixin
// * 2024/10/29 16:59
// */
//public abstract class BaseTransactionContextResource extends AbstractContext
//        implements Resource.Dynamic,
//        InsertPreparedStatementLoader,
//        UpdatePreparedStatementLoader,
//        SelectPreparedStatementLoader,
//        DeletePreparedStatementLoader {
//
//    protected static final String ROLLBACK = "rollback";
//    protected static final String COMMIT = "commit";
//    private static final String SET_NEXT_GTID = "set gtid_next = '%s'";
//    protected Connection connection;
//    protected Throwable lastUnbearable;
//    public long costTimeNS = 0;
//    protected boolean transactionTableConflict;
//
//
//    @InstanceResource
//    public TransactionTable transactionTable;
//
//
//
//
//    @Override
//    public Connection getConnection() {
//        return connection;
//    }
//
//
//}
