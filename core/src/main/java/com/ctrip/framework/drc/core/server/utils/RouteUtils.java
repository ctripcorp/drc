package com.ctrip.framework.drc.core.server.utils;

import com.ctrip.framework.drc.core.config.CommonConfig;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

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
    
    // use srcDc 
//    public static Route randomRoute(String currentDc, String tag, Integer orgId, String dstDc, List<Dc> dcs) {
//        List<Route> routes = filterRoutes(currentDc, tag, orgId, dstDc, dcs);
//        return random(routes);
//    }
//
//    public static List<Route> filterRoutes(String currentDc, String tag, Integer orgId, String dstDc, List<Dc> dcs) {
//        CommonConfig config = CommonConfig.getInstance();
//        Map<String, String> dc2regionMap = config.getDc2regionMap();
//        String currentRegion = dc2regionMap.get(currentDc);
//        String dstRegion = dc2regionMap.get(dstDc);
//
//        logger.debug("[randomRoute]currentRegion: {}, tag: {}, orgId, {}, dstRegion: {}", currentRegion, tag, orgId, dstRegion);
//        List<Route> routes = routes(currentRegion, tag, dcs);
//        List<Route> resultsCandidates = new LinkedList<>();
//        if(routes.isEmpty()){
//            return resultsCandidates;
//        }
//        logger.debug("[randomRoute]routes: {}", routes);
//        //for Same dstRegion
//        List<Route> dstRegionRoutes = new LinkedList<>();
//        routes.forEach(routeMeta -> {
//            if (routeMeta.getDstRegion().equalsIgnoreCase(dstRegion)) {
//                dstRegionRoutes.add(routeMeta);
//            }
//        });
//        if(dstRegionRoutes.isEmpty()){
//            logger.debug("[randomRoute]dst region empty: {}", routes);
//            return resultsCandidates;
//        }
//
//        //for same org id
//        dstRegionRoutes.forEach(routeMeta -> {
//            if(ObjectUtils.equals(routeMeta.getOrgId(), orgId)){
//                resultsCandidates.add(routeMeta);
//            }
//        });
//
//        if(!resultsCandidates.isEmpty()){
//            return resultsCandidates;
//        }
//
//        dstRegionRoutes.forEach(routeMeta -> {
//            Integer orgId1 = routeMeta.getOrgId();
//            if(orgId1 == null || orgId1 <= 0L){
//                resultsCandidates.add(routeMeta);
//            }
//        });
//
//        return resultsCandidates;
//    }
    
    
    // use srcRegion
    public static Route randomRoute(String currentRegion, String tag, Integer orgId, String dstRegion, List<Dc> dcs) {
        List<Route> routes = filterRoutes(currentRegion, tag, orgId, dstRegion, dcs);
        return random(routes);
    }
    
    public static List<Route> filterRoutes(String currentRegion, String tag, Integer orgId, String dstRegion, List<Dc> dcs) {
        logger.debug("[randomRoute]currentRegion: {}, tag: {}, orgId, {}, dstRegion: {}", currentRegion, tag, orgId, dstRegion);
        List<Route> routes = routes(currentRegion, tag, dcs);
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

    public static List<Route> routes(String srcRegion, String tag, List<Dc> dcs) {
        List<Route> result = new LinkedList<>();

        if (!CollectionUtils.isEmpty(dcs)) {
            dcs.stream().flatMap(dc -> dc.getRoutes().stream()).forEach(
                    route -> {
                        if (route.tagEquals(tag) && srcRegion.equalsIgnoreCase(route.getSrcRegion())) {
                            result.add(MetaClone.clone(route));
                        }
                    }
            );
        }
        return result;
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
