package com.ctrip.framework.drc.fetcher;

import com.ctrip.framework.drc.fetcher.activity.event.DumpEventActivityTest;
import com.ctrip.framework.drc.fetcher.activity.event.GroupActivityTest;
import com.ctrip.framework.drc.fetcher.activity.event.InvolveActivityTest;
import com.ctrip.framework.drc.fetcher.activity.event.LoadEventActivityTest;
import com.ctrip.framework.drc.fetcher.activity.monitor.ReportActivityTest;
import com.ctrip.framework.drc.fetcher.activity.replicator.driver.FetcherConnectionTest;
import com.ctrip.framework.drc.fetcher.activity.replicator.handler.command.FetcherBinlogDumpGtidCommandHandlerTest;
import com.ctrip.framework.drc.fetcher.event.*;
import com.ctrip.framework.drc.fetcher.resource.condition.CapacityResourceTest;
import com.ctrip.framework.drc.fetcher.resource.condition.ListenableDirectMemoryResourceTest;
import com.ctrip.framework.drc.fetcher.resource.context.*;
import com.ctrip.framework.drc.fetcher.resource.thread.ExecutorResourceTest;
import com.ctrip.framework.drc.fetcher.system.*;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @Author limingdong
 * @create 2021/3/15
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TaskQueueActivityTest.class,

        NetworkContextResourceTest.class,
        DumpEventActivityTest.class,
        GroupActivityTest.class,
        InvolveActivityTest.class,
        LoadEventActivityTest.class,
        ReportActivityTest.class,

        FetcherConnectionTest.class,
        FetcherBinlogDumpGtidCommandHandlerTest.class,

        FetcherEventGroupTest.class,

        MonitoredDeleteRowsEventTest.class,
        MonitoredDrcTableMapEventTest.class,
        MonitoredGtidLogEventTest.class,
        MonitoredQueryEventTest.class,
        MonitoredTableMapEventTest.class,
        MonitoredUpdateRowsEventTest.class,
        MonitoredWriteRowsEventTest.class,

        //Mechanism
        AbstractActivityTest.class,
        AbstractUnitTest.class,
        AbstractLoopActivityTest.class,
        AbstractSystemTest.class,
        AbstractLinkTest.class,

        //Resource
        CapacityResourceTest.class,
        ListenableDirectMemoryResourceTest.class,

        //Thread
        ExecutorResourceTest.class,

        //Context
        EventGroupContextTest.class,
        SequenceNumberContextTest.class,
        TableKeyContextTest.class,
        TimeTraceContextTest.class

})
public class AllTests {
}
