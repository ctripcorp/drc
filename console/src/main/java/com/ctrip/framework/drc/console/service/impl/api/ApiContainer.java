package com.ctrip.framework.drc.console.service.impl.api;

import com.ctrip.framework.drc.core.service.beacon.BeaconApiService;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.ctrip.framework.drc.core.service.mysql.MySQLToolsApiService;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.framework.drc.core.service.user.UserService;

/**
 * @ClassName ServiceObtain
 * @Author haodongPan
 * @Date 2021/11/24 15:49
 * @Version: $
 */
public class ApiContainer extends com.ctrip.xpipe.utils.ServicesUtil {
    
    private static class DbClusterApiServiceHolder {
        public static final DbClusterApiService INSTANCE = load(DbClusterApiService.class);
    }
    public static DbClusterApiService getDbClusterApiServiceImpl() {
        return DbClusterApiServiceHolder.INSTANCE;
    }
    
    
    private static class BeaconApiServiceHolder {
        public static final BeaconApiService INSTANCE = load(BeaconApiService.class);
    }
    public static BeaconApiService getBeaconApiServiceImpl() {
        return BeaconApiServiceHolder.INSTANCE;
    }
    
    
    private static class MySQLToolsApiServiceHolder {
        public static final MySQLToolsApiService INSTANCE = load(MySQLToolsApiService.class);
    }
    public static MySQLToolsApiService getMySQLToolsApiServiceImpl() {
        return MySQLToolsApiServiceHolder.INSTANCE;
    }


    private static class OPSApiServiceHolder {
        public static final OPSApiService INSTANCE = load(OPSApiService.class);
    }
    public static OPSApiService getOPSApiServiceImpl() {
        return OPSApiServiceHolder.INSTANCE;
    }
    
    private static class UserServiceHolder {
        public static final UserService INSTANCE = load(UserService.class);
    }
    public static  UserService getUserServiceImpl(){
        return UserServiceHolder.INSTANCE;
    }
    
}
