package com.ctrip.framework.drc.console.utils;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-14
 */
public class XmlUtils {

    public static String formatXML(String xml) throws DocumentException {
        if(null == xml) {
            return null;
        }
        Document document = DocumentHelper.parseText(xml);
        String result = document.asXML();
        int index = result.indexOf("\n");
        return result.substring(index+1);
    }
}
