package com.ctrip.framework.drc.manager.ha.multidc;

import com.ctrip.xpipe.tuple.Pair;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
public interface ApplierMasterChooserAlgorithm {

    Pair<String, Integer> choose();

}
