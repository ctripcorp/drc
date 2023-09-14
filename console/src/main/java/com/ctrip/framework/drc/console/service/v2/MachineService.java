package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.xpipe.api.endpoint.Endpoint;

public interface MachineService {
    Endpoint getMasterEndpointCached(String mha);

    Endpoint getMasterEndpoint(String mha);
}
