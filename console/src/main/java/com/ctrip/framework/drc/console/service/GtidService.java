package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dto.GtidFillDto;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-22
 */
public interface GtidService {

    JsonNode fillGtid(GtidFillDto gtidFillDto) throws Exception;
}
