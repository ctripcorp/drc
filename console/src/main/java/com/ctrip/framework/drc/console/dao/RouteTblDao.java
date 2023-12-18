package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.RouteTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

@Repository
public class RouteTblDao extends AbstractDao<RouteTbl> {

    public RouteTblDao() throws SQLException {
        super(RouteTbl.class);
    }

    public void upsert(Long routeOrgId, Long srcDcId, Long dstDcId, String srcProxyIds, String relayProxyIds, String dstProxyIds, String tag, Integer deleted) throws SQLException {
        RouteTbl routeTbl = queryAll().stream().filter(p -> routeOrgId.equals(p.getRouteOrgId()) && tag.equalsIgnoreCase(p.getTag()) && srcDcId.equals(p.getSrcDcId()) && dstDcId.equals(p.getDstDcId())).findFirst().orElse(null);
        if(null == routeTbl) {
            insertRoute(routeOrgId, srcDcId, dstDcId, srcProxyIds, relayProxyIds, dstProxyIds, tag);
        } else if ( (routeTbl.getDeleted() != null && !routeTbl.getDeleted().equals(deleted))
                || !srcProxyIds.equalsIgnoreCase(routeTbl.getSrcProxyIds())
                || !dstProxyIds.equalsIgnoreCase(routeTbl.getDstProxyIds())
                || !relayProxyIds.equalsIgnoreCase(routeTbl.getOptionalProxyIds())) {
            if (null == deleted) {
                routeTbl.setDeleted(BooleanEnum.FALSE.getCode());
            } else {
                routeTbl.setDeleted(deleted);
            }
            routeTbl.setSrcProxyIds(srcProxyIds);
            routeTbl.setDstProxyIds(dstProxyIds);
            routeTbl.setOptionalProxyIds(relayProxyIds);
            update(routeTbl);
        }
    }

    public void insertRoute(Long routeOrgId, Long srcDcId, Long dstDcId, String srcProxyIds, String relayProxyIds, String dstProxyIds, String tag) throws SQLException {
        RouteTbl routeTbl = RouteTbl.createRoutePojo(routeOrgId, srcDcId, dstDcId, srcProxyIds, relayProxyIds, dstProxyIds, tag);
        insert(routeTbl);
    }
}