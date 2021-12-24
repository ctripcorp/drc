package com.ctrip.framework.drc.fetcher.system;

import com.ctrip.xpipe.api.lifecycle.Disposable;
import com.ctrip.xpipe.api.lifecycle.Initializable;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

/**
 * @Author Slight
 * Sep 18, 2019
 */
public interface Activity extends Unit, Initializable, Disposable, Startable, Stoppable {
}
