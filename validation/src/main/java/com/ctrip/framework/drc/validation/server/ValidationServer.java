package com.ctrip.framework.drc.validation.server;

import com.ctrip.framework.drc.fetcher.activity.event.InvolveActivity;
import com.ctrip.framework.drc.fetcher.activity.event.LoadEventActivity;
import com.ctrip.framework.drc.fetcher.resource.condition.CapacityResource;
import com.ctrip.framework.drc.fetcher.resource.condition.ListenableDirectMemoryResource;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContextResource;
import com.ctrip.framework.drc.fetcher.resource.thread.ExecutorResource;
import com.ctrip.framework.drc.fetcher.system.AbstractLink;
import com.ctrip.framework.drc.validation.activity.event.ValidationActivity;
import com.ctrip.framework.drc.validation.activity.event.ValidationDumpEventActivity;
import com.ctrip.framework.drc.validation.activity.event.ValidationGroupActivity;
import com.ctrip.framework.drc.validation.activity.monitor.ValidationMetricsActivity;
import com.ctrip.framework.drc.validation.activity.monitor.ValidationResultActivity;

/**
 * @author shenhaibo
 * @version 1.0
 * 2021/3/11
 */
public class ValidationServer extends AbstractLink {

    public void define() throws Exception {
        source(ValidationDumpEventActivity.class)
                .with(ExecutorResource.class)
                .with(LinkContextResource.class)
                .with(CapacityResource.class)
                .with(ListenableDirectMemoryResource.class)
                .with(ValidationMetricsActivity.class)
                .with(ValidationResultActivity.class)
                .with(LoadEventActivity.class)
                .link(InvolveActivity.class)
                .link(ValidationGroupActivity.class)
                .link(ValidationActivity.class, 100);
        check();
    }


}
