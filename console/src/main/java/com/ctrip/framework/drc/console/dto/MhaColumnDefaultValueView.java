package com.ctrip.framework.drc.console.dto;

import java.util.List;

/**
 * Created by dengquanliang
 * 2024/12/10 19:22
 */
public class MhaColumnDefaultValueView {
    private List<MhaColumnDefaultValueDto> mhaColumns;
    private List<String> errorMhas;

    public MhaColumnDefaultValueView() {
    }

    public MhaColumnDefaultValueView(List<MhaColumnDefaultValueDto> mhaColumns, List<String> errorMhas) {
        this.mhaColumns = mhaColumns;
        this.errorMhas = errorMhas;
    }

    public List<MhaColumnDefaultValueDto> getMhaColumns() {
        return mhaColumns;
    }

    public void setMhaColumns(List<MhaColumnDefaultValueDto> mhaColumns) {
        this.mhaColumns = mhaColumns;
    }

    public List<String> getErrorMhas() {
        return errorMhas;
    }

    public void setErrorMhas(List<String> errorMhas) {
        this.errorMhas = errorMhas;
    }

    @Override
    public String toString() {
        return "MhaColumnDefaultValueView{" +
                "mhaColumns=" + mhaColumns +
                ", errorMhas=" + errorMhas +
                '}';
    }
}
