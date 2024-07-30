package com.ctrip.framework.drc.console.service.v2.security;

import com.ctrip.framework.drc.console.param.v2.security.Account;
import java.sql.SQLException;

public interface KmsService {
    
    
    // might fail, retry if necessary
    String getSecretKey(String accessToken);
    
    Account getAccountInfo(String accessToken);
    
}
