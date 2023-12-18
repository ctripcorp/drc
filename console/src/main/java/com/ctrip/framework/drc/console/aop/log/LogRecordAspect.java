package com.ctrip.framework.drc.console.aop.log;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.log.entity.OperationLogTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.log.LogRecordService;
import com.ctrip.framework.drc.core.service.user.UserService;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Timestamp;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.annotation.Order;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.stereotype.Component;

/**
 * @ClassName LogRecordAspect
 * @Author haodongPan
 * @Date 2023/12/6 16:06
 * @Version: $
 */
@Order(2)
@Aspect
@Component
public class LogRecordAspect {

    @Autowired
    private LogRecordService logRecordService;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    private UserService userService = ApiContainer.getUserServiceImpl();
    private final Logger logger = LoggerFactory.getLogger("OPERATION");
    private static final Pattern pattern = Pattern.compile("\\{(.*?)}");
    private SpelExpressionParser parser = new SpelExpressionParser();

    @Pointcut("@annotation(com.ctrip.framework.drc.console.aop.log.LogRecord)")
    public void pointcut() {}

    @Around(value = "pointcut() && @annotation(logRecord)")
    public Object around(ProceedingJoinPoint joinPoint, LogRecord logRecord) throws Throwable {
        if (!shouldProxy(joinPoint)) {
            return joinPoint.proceed();
        }

        MethodExecuteContext methodExecuteContext = new MethodExecuteContext();
        try {
            beforeExecute(methodExecuteContext,joinPoint, logRecord);
        } catch (Throwable e) {
            logger.error("LogRecordAspect beforeExecute error", e);
        }
        
        try {
            Object result = joinPoint.proceed();
            methodExecuteContext.setResult(result);
            methodExecuteContext.setSuccess(true);
        } catch (Throwable e) {
            methodExecuteContext.setSuccess(false);
            methodExecuteContext.setThrowable(e);
        }
        
        try {
            afterExecute(methodExecuteContext,joinPoint, logRecord);
        } catch (Throwable e) {
            logger.error("LogRecordAspect afterExecute error", e);
        }
        
        if (methodExecuteContext.getThrowable() != null) {
            throw methodExecuteContext.getThrowable();
        }
        
        return methodExecuteContext.getResult();
    }

    private void beforeExecute(MethodExecuteContext methodExecuteContext,ProceedingJoinPoint joinPoint, LogRecord logRecord) {
        // record info before execute
        OperationLogTbl operationLogTbl = methodExecuteContext.getOperationLogTbl();
        operationLogTbl.setCreateTime(new Timestamp(System.currentTimeMillis()));
        operationLogTbl.setType(logRecord.type().getVal());
        operationLogTbl.setAttr(logRecord.attr().getVal());
        String operator = StringUtils.isBlank(logRecord.operator()) ? userService.getInfo() : logRecord.operator();
        operationLogTbl.setOperator(operator);
    }
    
    private void afterExecute(MethodExecuteContext methodExecuteContext,ProceedingJoinPoint joinPoint, LogRecord logRecord) {
        if (methodExecuteContext.isSuccess()) {
            if (StringUtils.isNotEmpty(logRecord.success())) {
                String operation = parseExpression(joinPoint, logRecord.success());
                OperationLogTbl operationLogTbl = methodExecuteContext.getOperationLogTbl();
                operationLogTbl.setOperation(operation);
                logRecordService.record(operationLogTbl, logRecord.persisted());
            }  
            return;
        }  
        if (StringUtils.isNotEmpty(logRecord.fail())) {
            String operation = parseExpression(joinPoint,logRecord.fail());
            OperationLogTbl operationLogTbl = methodExecuteContext.getOperationLogTbl();
            operationLogTbl.setOperation(operation);
            operationLogTbl.setFail(BooleanEnum.TRUE.getCode());
            logRecordService.record(operationLogTbl,logRecord.persisted());
        }
    }

    private String parseExpression(JoinPoint joinPoint,String spelExpression) {
        if (!spelExpression.contains("{")) {
          return spelExpression;  
        }
        Matcher matcher = pattern.matcher(spelExpression);
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        MethodBasedEvaluationContext methodBasedEvaluationContext = new MethodBasedEvaluationContext(
                joinPoint.getTarget(), method, joinPoint.getArgs(), new DefaultParameterNameDiscoverer());
        StringBuilder parsedStr = new StringBuilder();
        while (matcher.find()) {
            String expression = matcher.group(1);
            String expressionVal = parser.parseExpression(expression)
                    .getValue(methodBasedEvaluationContext, String.class);
            matcher.appendReplacement(parsedStr,
                    Matcher.quoteReplacement(expressionVal == null ? "" : expressionVal));
        }
        matcher.appendTail(parsedStr);
        return parsedStr.toString();
    }
    
    private boolean shouldProxy(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature)joinPoint.getSignature();
        Method method = signature.getMethod();
        boolean isPublic = Modifier.isPublic(method.getModifiers());
        boolean proxySwitch = consoleConfig.getOperationLogSwitch();
        return  proxySwitch && isPublic;
    }
    
}
