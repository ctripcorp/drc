package com.ctrip.framework.drc.core.service.security;

import com.ctrip.xpipe.api.lifecycle.Ordered;

public interface HeraldService extends Ordered {
    
    String getLocalHeraldToken();
    
    boolean heraldValidate(String heraldToken);

}
