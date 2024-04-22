package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import java.sql.SQLException;
import java.util.List;

public interface MachineService {
    Endpoint getMasterEndpointCached(String mha);

    Endpoint getMasterEndpoint(String mha);
    
    String getUuid(String ip,int port) throws SQLException;

    Integer correctUuid(String ip, Integer port, String uuid) throws SQLException;

    List<Endpoint> getMasterEndpointsInAllAccounts(String mha);
}
