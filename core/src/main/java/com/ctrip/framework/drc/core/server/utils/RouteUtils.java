package com.ctrip.framework.drc.core.server.utils;


import com.ctrip.framework.drc.core.config.RegionConfig;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
        RegionConfig regionConfig = RegionConfig.getInstance();
        Map<String, String> dc2regionMap = regionConfig.getDc2regionMap();
        String currentRegion = dc2regionMap.get(currentDc);
        String dstRegion = dc2regionMap.get(dstDc);

        logger.debug("[randomRoute]currentRegion: {}, tag: {}, orgId, {}, dstRegion: {}", currentRegion, tag, orgId, dstRegion);
        List<Route> routes = routes(currentRegion, tag, dc);
        List<Route> resultsCandidates = new LinkedList<>();
        if(routes.isEmpty()){
            return resultsCandidates;
        }
        logger.debug("[randomRoute]routes: {}", routes);
        //for Same dstRegion
        List<Route> dstRegionRoutes = new LinkedList<>();
        routes.forEach(routeMeta -> {
            if (routeMeta.getDstRegion().equalsIgnoreCase(dstRegion)) {
                dstRegionRoutes.add(routeMeta);
            }
        });
        if(dstRegionRoutes.isEmpty()){
            logger.debug("[randomRoute]dst region empty: {}", routes);
            return resultsCandidates;
        }

        //for same org id
        dstRegionRoutes.forEach(routeMeta -> {
            if(ObjectUtils.equals(routeMeta.getOrgId(), orgId)){
                resultsCandidates.add(routeMeta);
            }
        });

        if(!resultsCandidates.isEmpty()){
            return resultsCandidates;
        }

        dstRegionRoutes.forEach(routeMeta -> {
            Integer orgId1 = routeMeta.getOrgId();
            if(orgId1 == null || orgId1 <= 0L){
                resultsCandidates.add(routeMeta);
            }
        });

        return resultsCandidates;
    }

    public static List<Route> routes(String srcRegion, String tag, Dc dc) {
        List<Route> result = new LinkedList<>();

        if(dc != null) {
            List<Route> routes = dc.getRoutes();
            if(routes != null){
                routes.forEach(route -> {
                    if(route.tagEquals(tag) && srcRegion.equalsIgnoreCase(route.getSrcRegion())) {
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
