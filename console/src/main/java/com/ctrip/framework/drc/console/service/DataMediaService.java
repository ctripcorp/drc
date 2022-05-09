package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dto.DataMediaDto;
import org.springframework.web.bind.annotation.PathVariable;

import java.sql.SQLException;

public interface DataMediaService {
    
    String addDataMedia(DataMediaDto dataMediaDto) throws SQLException;
    
    String addDataMediaMapping(Long applierGroupId,  Long dataMediaId) throws SQLException;
    
}
