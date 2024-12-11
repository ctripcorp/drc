package com.ctrip.framework.drc.core.service.inquirer;

import java.util.List;
import java.util.concurrent.Future;

/**
 * @author yongnian
 * @create: 2024/5/20 20:58
 */
public interface InfoInquirer<T> {
    Future<List<T>> query(String ipAndPort);
}
