package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.enums.DlockEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;

import java.util.List;

/**
 * Created by shiruixin
 * 2025/4/21 15:56
 */
public interface DLockService {
    void tryLocks(List<String> mhaNames, DlockEnum dlock) throws ConsoleException;
    void unlockLocks(List<String> mhaNames, DlockEnum dlock) throws ConsoleException;
}