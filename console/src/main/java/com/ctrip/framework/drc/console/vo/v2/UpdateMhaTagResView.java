package com.ctrip.framework.drc.console.vo.v2;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by shiruixin
 * 2025/7/8 17:19
 */
public class UpdateMhaTagResView {
    List<String> success;
    List<String> fail;
    List<String> mQFailList = Lists.newArrayList();
    List<String> mKFailList = Lists.newArrayList();
    List<String> aFailList = Lists.newArrayList();
    List<String> skipList = Lists.newArrayList();

    public UpdateMhaTagResView(List<String> success, List<String> fail) {
        this.success = success;
        this.fail = fail;
    }

    public UpdateMhaTagResView(List<String> success, List<String> fail, List<String> mQFailList, List<String> mKFailList, List<String> aFailList, List<String> skipList) {
        this.success = success;
        this.fail = fail;
        this.mQFailList = mQFailList;
        this.mKFailList = mKFailList;
        this.aFailList = aFailList;
        this.skipList = skipList;
    }

    public List<String> getSuccess() {
        return success;
    }

    public void setSuccess(List<String> success) {
        this.success = success;
    }

    public List<String> getFail() {
        return fail;
    }

    public void setFail(List<String> fail) {
        this.fail = fail;
    }

    public List<String> getmQFailList() {
        return mQFailList;
    }

    public void setmQFailList(List<String> mQFailList) {
        this.mQFailList = mQFailList;
    }

    public List<String> getmKFailList() {
        return mKFailList;
    }

    public void setmKFailList(List<String> mKFailList) {
        this.mKFailList = mKFailList;
    }

    public List<String> getaFailList() {
        return aFailList;
    }

    public void setaFailList(List<String> aFailList) {
        this.aFailList = aFailList;
    }

    public List<String> getSkipList() {
        return skipList;
    }

    public void setSkipList(List<String> skipList) {
        this.skipList = skipList;
    }
}
