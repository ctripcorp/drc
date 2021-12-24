package com.ctrip.framework.drc.core.server.utils;

import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * @Author: hbshen
 * @Date: 2021/4/19
 */
public class RouteUtils {

    public static Logger logger = LoggerFactory.getLogger(RouteUtils.class);

    public static Route randomRoute(String currentDc, String tag, Integer orgId, String dstDc, Dc dc) {
        List<Route> routes = filterRoutes(currentDc, tag, orgId, dstDc, dc);
        return random(routes);
    }

    public static List<Route> filterRoutes(String currentDc, String tag, Integer orgId, String dstDc, Dc dc) {
        logger.debug("[randomRoute]currentDc: {}, tag: {}, orgId, {}, dstDc: {}", currentDc, tag, orgId, dstDc);
        List<Route> routes = routes(currentDc, tag, dc);
        List<Route> resultsCandidates = new LinkedList<>();
        if(routes.isEmpty()){
            return resultsCandidates;
        }
        logger.debug("[randomRoute]routes: {}", routes);
        //for Same dstdc
        List<Route> dstDcRoutes = new LinkedList<>();
        routes.forEach(routeMeta -> {
            if(routeMeta.getDstDc().equalsIgnoreCase(dstDc)){
                dstDcRoutes.add(routeMeta);
            }
        });
        if(dstDcRoutes.isEmpty()){
            logger.debug("[randomRoute]dst dc empty: {}", routes);
            return resultsCandidates;
        }

        //for same org id
        dstDcRoutes.forEach(routeMeta -> {
            if(ObjectUtils.equals(routeMeta.getOrgId(), orgId)){
                resultsCandidates.add(routeMeta);
            }
        });

        if(!resultsCandidates.isEmpty()){
            return resultsCandidates;
        }

        dstDcRoutes.forEach(routeMeta -> {
            Integer orgId1 = routeMeta.getOrgId();
            if(orgId1 == null || orgId1 <= 0L){
                resultsCandidates.add(routeMeta);
            }
        });

        return resultsCandidates;
    }

    public static List<Route> routes(String srcDc, String tag, Dc dc) {
        List<Route> result = new LinkedList<>();

        if(dc != null) {
            List<Route> routes = dc.getRoutes();
            if(routes != null){
                routes.forEach(route -> {
                    if(route.tagEquals(tag) && srcDc.equalsIgnoreCase(route.getSrcDc())) {
                        result.add(MetaClone.clone(route));
                    }
                });
            }
        }

        return result;
    }

    public static <T> T random(List<T> resultsCandidates) {

        if(resultsCandidates.isEmpty()){
            return null;
        }
        int random = new Random().nextInt(resultsCandidates.size());
        logger.debug("[randomRoute]random: {}, size: {}", random, resultsCandidates.size());
        return resultsCandidates.get(random);

    }
}
