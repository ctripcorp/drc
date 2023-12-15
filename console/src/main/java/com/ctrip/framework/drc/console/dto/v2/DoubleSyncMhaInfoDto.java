package com.ctrip.framework.drc.console.dto.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.utils.MultiKey;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/12/14 18:01
 */
public class DoubleSyncMhaInfoDto {
    /**
     * MultiKey：srcMhaId, dstMhaId
     */
    private List<MultiKey> doubleSyncMultiKeys;
    private List<MhaTblV2> mhaTblV2s;

    public DoubleSyncMhaInfoDto(List<MultiKey> doubleSyncMultiKeys, List<MhaTblV2> mhaTblV2s) {
        this.doubleSyncMultiKeys = doubleSyncMultiKeys;
        this.mhaTblV2s = mhaTblV2s;
    }

    public DoubleSyncMhaInfoDto() {
    }

    public List<MultiKey> getDoubleSyncMultiKeys() {
        return doubleSyncMultiKeys;
    }

    public void setDoubleSyncMultiKeys(List<MultiKey> doubleSyncMultiKeys) {
        this.doubleSyncMultiKeys = doubleSyncMultiKeys;
    }

    public List<MhaTblV2> getMhaTblV2s() {
        return mhaTblV2s;
    }

    public void setMhaTblV2s(List<MhaTblV2> mhaTblV2s) {
        this.mhaTblV2s = mhaTblV2s;
    }
}
