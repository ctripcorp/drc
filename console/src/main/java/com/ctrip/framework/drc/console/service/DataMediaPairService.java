package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.core.meta.MessengerProperties;

import java.sql.SQLException;

public interface DataMediaPairService {
    
    // db -> mq
    MessengerProperties generateMessengerProperties(Long messengerGroupId) throws SQLException;
}
