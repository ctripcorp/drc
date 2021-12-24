package com.ctrip.framework.drc.console.utils;


import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import org.dom4j.DocumentException;
import org.junit.Assert;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.IOException;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-15
 */
public class XmlUtilsTest {
    @Test
    public void testFormatXml() throws IOException, SAXException {
        String res = null;

        String xml = null;
        try {
            res = XmlUtils.formatXML(xml);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof DocumentException);
        }
        Assert.assertNull(res);

        xml = "";
        res = null;
        try {
            res = XmlUtils.formatXML(xml);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof DocumentException);
        }
        Assert.assertNull(res);

        String xmlString = "<?xml version=\"1.0\" encoding=\"utf-8\"?><drc><dc id=\"shaoy\"><clusterManager ip=\"10.2.84.122\" port=\"8080\" master=\"true\"/><zkServer address=\"10.2.84.112:2181\"/><dbClusters><dbCluster id=\"integration-test.drcOy\" name=\"integration-test\" mhaName=\"drcOy\" buName=\"BBZ\" appId=\"100024819\"><dbs readUser=\"root\" readPassword=\"root\" writeUser=\"root\" writePassword=\"root\" monitorUser=\"root\" monitorPassword=\"root\"><db ip=\"10.2.83.109\" port=\"3306\" master=\"true\" uuid=\"84868488-c72f-11ea-bd40-fa163e02998c\"/><db ip=\"10.2.83.110\" port=\"3306\" master=\"false\" uuid=\"779e476f-c72f-11ea-b46a-fa163ec90ff6\"/></dbs><replicatorMonitor ip=\"10.2.83.113\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"\"/><replicator ip=\"10.2.83.105\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"84868488-c72f-11ea-bd40-fa163e02998c:1,f461da06-c72f-11ea-9c05-fa163eaa9d69:1-30\"/><applier ip=\"10.2.83.100\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb\" gtidExecuted=\"84868488-c72f-11ea-bd40-fa163e02998c:1-30,f461da06-c72f-11ea-9c05-fa163eaa9d69:1\"/><applier ip=\"10.2.86.137\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb\" gtidExecuted=\"84868488-c72f-11ea-bd40-fa163e02998c:1-30,f461da06-c72f-11ea-9c05-fa163eaa9d69:1\"/></dbCluster></dbClusters></dc><dc id=\"sharb\"><clusterManager ip=\"10.2.84.109\" port=\"8080\" master=\"true\"/><zkServer address=\"10.2.83.114:2181\"/><dbClusters><dbCluster id=\"integration-test.drcRb\" name=\"integration-test\" mhaName=\"drcRb\" buName=\"BBZ\" appId=\"100024819\"><dbs readUser=\"root\" readPassword=\"root\" writeUser=\"root\" writePassword=\"root\" monitorUser=\"root\" monitorPassword=\"root\"><db ip=\"10.2.83.107\" port=\"3306\" master=\"true\" uuid=\"f461da06-c72f-11ea-9c05-fa163eaa9d69\"/><db ip=\"10.2.83.108\" port=\"3306\" master=\"false\" uuid=\"bf750753-c72f-11ea-bae6-fa163e2d32b8\"/></dbs><replicatorMonitor ip=\"10.2.83.112\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"\"/><replicator ip=\"10.2.83.106\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"84868488-c72f-11ea-bd40-fa163e02998c:1-30,f461da06-c72f-11ea-9c05-fa163eaa9d69:1\"/><replicator ip=\"10.2.86.199\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"84868488-c72f-11ea-bd40-fa163e02998c:1-30,f461da06-c72f-11ea-9c05-fa163eaa9d69:1\"/><applier ip=\"10.2.83.111\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy\" gtidExecuted=\"84868488-c72f-11ea-bd40-fa163e02998c:1,f461da06-c72f-11ea-9c05-fa163eaa9d69:1-30\"/><applier ip=\"10.2.86.136\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy\" gtidExecuted=\"84868488-c72f-11ea-bd40-fa163e02998c:1,f461da06-c72f-11ea-9c05-fa163eaa9d69:1-30\"/></dbCluster></dbClusters></dc></drc>";
        xml =  DefaultSaxParser.parse(xmlString).toString();
        String expected = "<drc>\n" +
                "   <dc id=\"shaoy\">\n" +
                "      <clusterManager ip=\"10.2.84.122\" port=\"8080\" master=\"true\"/>\n" +
                "      <zkServer address=\"10.2.84.112:2181\"/>\n" +
                "      <dbClusters>\n" +
                "         <dbCluster id=\"integration-test.drcOy\" name=\"integration-test\" mhaName=\"drcOy\" buName=\"BBZ\" appId=\"100024819\">\n" +
                "            <dbs readUser=\"root\" readPassword=\"root\" writeUser=\"root\" writePassword=\"root\" monitorUser=\"root\" monitorPassword=\"root\">\n" +
                "               <db ip=\"10.2.83.109\" port=\"3306\" master=\"true\" uuid=\"84868488-c72f-11ea-bd40-fa163e02998c\"/>\n" +
                "               <db ip=\"10.2.83.110\" port=\"3306\" master=\"false\" uuid=\"779e476f-c72f-11ea-b46a-fa163ec90ff6\"/>\n" +
                "            </dbs>\n" +
                "            <replicatorMonitor ip=\"10.2.83.113\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"\" master=\"false\"/>\n" +
                "            <replicator ip=\"10.2.83.105\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"84868488-c72f-11ea-bd40-fa163e02998c:1,f461da06-c72f-11ea-9c05-fa163eaa9d69:1-30\" master=\"false\"/>\n" +
                "            <applier ip=\"10.2.83.100\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb\" gtidExecuted=\"84868488-c72f-11ea-bd40-fa163e02998c:1-30,f461da06-c72f-11ea-9c05-fa163eaa9d69:1\" master=\"false\"/>\n" +
                "            <applier ip=\"10.2.86.137\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb\" gtidExecuted=\"84868488-c72f-11ea-bd40-fa163e02998c:1-30,f461da06-c72f-11ea-9c05-fa163eaa9d69:1\" master=\"false\"/>\n" +
                "         </dbCluster>\n" +
                "      </dbClusters>\n" +
                "   </dc>\n" +
                "   <dc id=\"sharb\">\n" +
                "      <clusterManager ip=\"10.2.84.109\" port=\"8080\" master=\"true\"/>\n" +
                "      <zkServer address=\"10.2.83.114:2181\"/>\n" +
                "      <dbClusters>\n" +
                "         <dbCluster id=\"integration-test.drcRb\" name=\"integration-test\" mhaName=\"drcRb\" buName=\"BBZ\" appId=\"100024819\">\n" +
                "            <dbs readUser=\"root\" readPassword=\"root\" writeUser=\"root\" writePassword=\"root\" monitorUser=\"root\" monitorPassword=\"root\">\n" +
                "               <db ip=\"10.2.83.107\" port=\"3306\" master=\"true\" uuid=\"f461da06-c72f-11ea-9c05-fa163eaa9d69\"/>\n" +
                "               <db ip=\"10.2.83.108\" port=\"3306\" master=\"false\" uuid=\"bf750753-c72f-11ea-bae6-fa163e2d32b8\"/>\n" +
                "            </dbs>\n" +
                "            <replicatorMonitor ip=\"10.2.83.112\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"\" master=\"false\"/>\n" +
                "            <replicator ip=\"10.2.83.106\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"84868488-c72f-11ea-bd40-fa163e02998c:1-30,f461da06-c72f-11ea-9c05-fa163eaa9d69:1\" master=\"false\"/>\n" +
                "            <replicator ip=\"10.2.86.199\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"84868488-c72f-11ea-bd40-fa163e02998c:1-30,f461da06-c72f-11ea-9c05-fa163eaa9d69:1\" master=\"false\"/>\n" +
                "            <applier ip=\"10.2.83.111\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy\" gtidExecuted=\"84868488-c72f-11ea-bd40-fa163e02998c:1,f461da06-c72f-11ea-9c05-fa163eaa9d69:1-30\" master=\"false\"/>\n" +
                "            <applier ip=\"10.2.86.136\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy\" gtidExecuted=\"84868488-c72f-11ea-bd40-fa163e02998c:1,f461da06-c72f-11ea-9c05-fa163eaa9d69:1-30\" master=\"false\"/>\n" +
                "         </dbCluster>\n" +
                "      </dbClusters>\n" +
                "   </dc>\n" +
                "</drc>";
        res = null;
        try {
            res = XmlUtils.formatXML(xml);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof DocumentException);
        }
        Assert.assertEquals(expected, res);
    }
}