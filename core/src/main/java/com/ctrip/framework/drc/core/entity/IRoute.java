package com.ctrip.framework.drc.core.entity;

import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author chen.zhu
 * <p>
 * Jun 05, 2018
 */
public interface IRoute {

    String TAG_META = "meta";
    String TAG_CONSOLE = "console";
    String ROUTE = "ROUTE";

    default boolean tagEquals(String tag){
        return ObjectUtils.equals(tag, getTag(), ((obj1, obj2) -> obj1.equalsIgnoreCase(obj2)));
    }

    String getRouteInfo();

    String getTag();

    default String routeProtocol(){

        String routeInfo = getRouteInfo();
        if(StringUtil.isEmpty(routeInfo)){
            return "";
        }
        return String.format("%s %s %s", ProxyProtocol.KEY_WORD, ROUTE, routeInfo.trim());
    }
}
