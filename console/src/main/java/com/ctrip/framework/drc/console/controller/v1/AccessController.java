package com.ctrip.framework.drc.console.controller.v1;


import com.ctrip.framework.drc.console.service.SSOService;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 * @author maojiawei
 * @version 1.0
 * date: 2020-07-28
 *  todo add token check
 */
@RestController
@RequestMapping("/api/drc/v1/access/")
public class AccessController {

    
    @Autowired
    private SSOService ssoServiceImpl;
    
    
    @PostMapping("sso/degrade/switch/{isOpen}") 
    public ApiResult changeAllServerSSODegradeStatus(@PathVariable Boolean isOpen) {
        return ssoServiceImpl.degradeAllServer(isOpen);
    }

    
    @PostMapping("sso/degrade/notify/{isOpen}")
    public ApiResult changeLocalServerSSODegradeStatus(@PathVariable Boolean isOpen) {
        return ssoServiceImpl.setDegradeSwitch(isOpen);
    }
    
}
