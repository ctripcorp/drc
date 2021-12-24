package com.ctrip.framework.drc.core.service.exceptions;


public class UnexpectedEnumValueException extends RuntimeException{

    public UnexpectedEnumValueException(String msg){
        super(msg);
    }
}
