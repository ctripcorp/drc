package com.ctrip.framework.drc.console.service.v2.security;

import com.ctrip.framework.drc.console.param.v2.security.MhaAccounts;

// work in all region, get account info from meta xml
public interface MetaAccountService {
    
    MhaAccounts getMhaAccounts(String mhaName);
}
