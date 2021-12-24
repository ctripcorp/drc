package com.ctrip.framework.drc.manager.controller.applier;

import com.ctrip.framework.drc.core.server.config.cm.dto.SchemasHistoryDeltaDto;
import com.ctrip.framework.drc.core.server.config.cm.dto.SchemasHistoryDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @Author Slight
 * Nov 19, 2019
 */
@RestController
@RequestMapping("/applier/{id}")
public class SchemasHistoryController {

    @Autowired
    SchemasHistoryComponent schemasHistory;

    @RequestMapping(value = "schema", method = RequestMethod.PUT)
    public void add(@PathVariable String id, @RequestBody SchemasHistoryDeltaDto schemasHistoryDeltaDto) throws Exception {
        schemasHistory.merge(id, schemasHistoryDeltaDto);
    }

    @RequestMapping(value = "schema", method = RequestMethod.GET)
    public SchemasHistoryDto getAll(@PathVariable String id) throws Exception{
        return schemasHistory.fetch(id);
    }
}
