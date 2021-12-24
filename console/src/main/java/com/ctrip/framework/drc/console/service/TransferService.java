package com.ctrip.framework.drc.console.service;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-08-11
 */
public interface TransferService {
    void loadMetaData(String meta) throws Exception;
    void loadOneMetaData(String oneMetaData) throws Exception;
}
