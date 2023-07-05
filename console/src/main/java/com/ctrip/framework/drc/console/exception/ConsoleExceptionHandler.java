package com.ctrip.framework.drc.console.exception;

import com.ctrip.framework.drc.core.http.ApiResult;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

/**
 * Created by dengquanliang
 * 2023/7/5 14:48
 */
@ControllerAdvice
public class ConsoleExceptionHandler {

    @ExceptionHandler(value = ConsoleException.class)
    public ApiResult consoleExceptionHandler(ConsoleException consoleException) {
        return ApiResult.getFailInstance(null, consoleException.getMessage());
    }
}
