package com.ctrip.framework.drc.console.utils;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public static String replaceBlank(String str) {
        String dest = "";
        if (str != null) {
            Pattern p = Pattern.compile("\\s*|\t|\r|\n");
            Matcher m = p.matcher(str);
            dest = m.replaceAll("");
        }
        return dest;
    }
}
