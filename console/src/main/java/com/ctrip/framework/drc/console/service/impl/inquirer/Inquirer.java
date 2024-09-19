package com.ctrip.framework.drc.console.service.impl.inquirer;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Created by shiruixin
 * 2024/9/11 17:32
 */
public interface Inquirer<T> {
    Future<List<T>> query(String ipAndPort);
}
