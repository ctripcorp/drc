package com.ctrip.framework.drc.console.utils;

import com.ctrip.framework.drc.console.enums.IErrorDef;
import com.ctrip.framework.drc.console.exception.ConsoleException;

/**
 * Created by dengquanliang
 * 2023/7/5 14:46
 */
public class ConsoleExceptionUtils {

    public static ConsoleException message(String message) {
        return new ConsoleException(message);
    }

    public static ConsoleException message(IErrorDef errorDef) {
        return new ConsoleException(errorDef.getMessage());
    }

    public static ConsoleException message(IErrorDef errorDef, String extraMsg) {
        return new ConsoleException(String.join("|", errorDef.getMessage(), extraMsg));
    }

    public static ConsoleException message(IErrorDef errorDef, Throwable e) {
        return new ConsoleException(errorDef.getMessage(), e);
    }
}
