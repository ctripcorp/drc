package com.ctrip.framework.drc.core.server;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * @author wenchao.meng
 * <p>
 * Aug 22, 2019
 */
public interface DrcServer extends Lifecycle {

    Endpoint getUpstreamMaster();

}
