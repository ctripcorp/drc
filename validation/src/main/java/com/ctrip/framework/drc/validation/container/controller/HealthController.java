package com.ctrip.framework.drc.validation.container.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthController {

    @RequestMapping("/health")
    public boolean checkHealthStatus() {
        return true;
    }
}