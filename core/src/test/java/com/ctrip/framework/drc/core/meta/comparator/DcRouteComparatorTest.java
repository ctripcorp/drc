package com.ctrip.framework.drc.core.meta.comparator;

import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.utils.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;

/**
 * @Author: hbshen
 * @Date: 2021/4/15
 */
public class DcRouteComparatorTest {

    private DcRouteComparator comparator;

    private Dc current;

    private Dc future;

    private String comparatorFile = "comparator.xml";

    @Before
    public void beforeDcRouteMetaComparatorTest() throws IOException, SAXException {
        current = getDcMeta("jq");
        future = MetaClone.clone(current);
        Assert.assertFalse(current.getDbClusters().isEmpty());
    }

    @Test
    public void testCompareWithNoChanges() {
        comparator = new DcRouteComparator(current, future);
        comparator.compare();

        Assert.assertTrue(comparator.getAdded().isEmpty());
        Assert.assertTrue(comparator.getRemoved().isEmpty());
        Assert.assertTrue(comparator.getMofified().isEmpty());
    }

    @Test
    public void testCompareWithRouteAdd() {
        // case 1: by implementation, if the route doesn't contain tag as Route.TAG_META, i.e. "meta", it will be filtered during comparision
        future.addRoute(new Route(1000).setRouteInfo("PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.7:443").setSrcDc("jq").setDstDc("fra"));
        comparator = new DcRouteComparator(current, future);
        comparator.compare();

        Assert.assertTrue(comparator.getAdded().isEmpty());
        Assert.assertTrue(comparator.getRemoved().isEmpty());
        Assert.assertTrue(comparator.getMofified().isEmpty());

        future.addRoute(new Route(2000).setTag(Route.TAG_META));
        comparator = new DcRouteComparator(current, future);
        comparator.compare();

        Assert.assertFalse(comparator.getAdded().isEmpty());
        Assert.assertTrue(comparator.getRemoved().isEmpty());
        Assert.assertTrue(comparator.getMofified().isEmpty());
    }

    @Test
    public void testCompareWithRouteDeleted() {
        future.getRoutes().remove(0);
        comparator = new DcRouteComparator(current, future);
        comparator.compare();

        Assert.assertTrue(comparator.getAdded().isEmpty());
        Assert.assertFalse(comparator.getRemoved().isEmpty());
        Assert.assertTrue(comparator.getMofified().isEmpty());
    }

    @Test
    public void testCompareWithModified() {
        future.getRoutes().get(0).setRouteInfo("PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.7:443");
        comparator = new DcRouteComparator(current, future);
        comparator.compare();

        Assert.assertTrue(comparator.getAdded().isEmpty());
        Assert.assertTrue(comparator.getRemoved().isEmpty());
        Assert.assertFalse(comparator.getMofified().isEmpty());

        Assert.assertEquals(1, comparator.getMofified().size());
    }

    protected Dc getDcMeta(String dc) throws IOException, SAXException {
        return getDrcMeta().getDcs().get(dc);
    }

    private Drc getDrcMeta() throws IOException, SAXException {
        return loadDrcMeta(getDrcMetaConfigFile());
    }

    private String getDrcMetaConfigFile() {
        return comparatorFile;
    }

    protected Drc loadDrcMeta(String configFile) throws SAXException, IOException {
        if (configFile == null) {
            return null;
        }
        InputStream ins = FileUtils.getFileInputStream(configFile, getClass());
        return DefaultSaxParser.parse(ins);
    }
}
