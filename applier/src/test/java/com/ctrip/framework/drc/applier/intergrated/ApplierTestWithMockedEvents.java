package com.ctrip.framework.drc.applier.intergrated;

import com.ctrip.framework.drc.applier.event.*;
import com.ctrip.framework.drc.applier.server.LocalApplierServer;
import com.ctrip.framework.drc.core.config.TestConfig;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.fetcher.activity.event.InvolveActivity;
import com.ctrip.framework.drc.fetcher.event.ApplierXidEvent;
import com.ctrip.framework.drc.fetcher.system.AbstractActivity;
import com.ctrip.framework.drc.fetcher.system.InstanceActivity;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.List;

import static org.mockito.Mockito.*;

/**
 * @Author Slight
 * Sep 30, 2019
 */
public class ApplierTestWithMockedEvents implements ApplierColumnsRelatedTest {

    @Test
    public void runApplierTestWithMockedEvents() throws Exception {
        EmbeddedApplierServer server = new EmbeddedApplierServer();
        server.initialize();
        server.start();
        Thread.sleep(100);
        server.stop();
        server.dispose();
    }

    public class MockEventActivity extends AbstractActivity {

        @InstanceActivity
        public InvolveActivity entry;

        public void doStart() throws Exception {
            ApplierDrcTableMapEvent drcTableMapEvent = spy(new ApplierDrcTableMapEvent());
            when(drcTableMapEvent.getColumns()).thenReturn(columns1());
            when(drcTableMapEvent.getSchemaName()).thenReturn("prod");
            when(drcTableMapEvent.getTableName()).thenReturn("hello");

            entry.waitSubmit(drcTableMapEvent);

            //first group
            entry.waitSubmit(new MockGtidEvent("a0780e56-d445-11e9-97b4-58a328e0e9f2:101", 1, 2));
            entry.waitSubmit(new MockTableMapEvent("prod", "hello"));

            DecryptedWriteRowsEvent writeRowsEvent = spy(new DecryptedWriteRowsEvent(null));
            when(writeRowsEvent.getBeforePresentRowsValues()).thenReturn(Lists.<List<Object>>newArrayList(
                    Lists.newArrayList(1, "Phy", "Male", "1:00"),
                    Lists.newArrayList(2, "Mag", "Male", "2:00"),
                    Lists.newArrayList(3, "Sli", "Male", "3:00")
            ));
            when(writeRowsEvent.getBeforeRowsKeysPresent()).thenReturn(Lists.newArrayList(true, true, true, true));
            doNothing().when(writeRowsEvent).load(any());
            entry.waitSubmit(writeRowsEvent);
            entry.waitSubmit(new ApplierXidEvent());

            //second group
            entry.waitSubmit(new MockGtidEvent("a0780e56-d445-11e9-97b4-58a328e0e9f2:102", 2, 3));
            entry.waitSubmit(new MockTableMapEvent("prod", "hello"));

            DecryptedDeleteRowsEvent deleteRowsEvent = spy(new DecryptedDeleteRowsEvent(null));
            when(deleteRowsEvent.getBeforePresentRowsValues()).thenReturn(Lists.<List<Object>>newArrayList(
                    Lists.newArrayList(1, "FAKE", "FAKE", "1:00"),
                    Lists.newArrayList(2, "FAKE", "FAKE", "2:00")
            ));
            when(deleteRowsEvent.getBeforeRowsKeysPresent()).thenReturn(Lists.newArrayList(true, true, true, true));
            doNothing().when(deleteRowsEvent).load(any());
            entry.waitSubmit(deleteRowsEvent);
            entry.waitSubmit(new ApplierXidEvent());
        }

        @Override
        public void doStop() throws Exception {

        }
    }

    public class EmbeddedApplierServer extends LocalApplierServer {
        public EmbeddedApplierServer() throws Exception {
            super(3306, 8383, SystemConfig.INTEGRITY_TEST, Sets.newHashSet(), new TestConfig(ApplyMode.set_gtid, null));
            MockEventActivity mock = new MockEventActivity();
            mock.setSystem(this);
            activities.put("DumpEventActivity", mock);
        }
    }
}
