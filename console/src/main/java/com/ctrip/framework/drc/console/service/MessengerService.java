package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.core.entity.Messenger;

import java.sql.SQLException;
import java.util.List;

public interface MessengerService {
    
    List<Messenger> generateMessengers (Long mhaId) throws SQLException;
}
