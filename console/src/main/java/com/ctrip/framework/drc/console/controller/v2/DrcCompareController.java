package com.ctrip.framework.drc.console.controller.v2;


import com.ctrip.framework.drc.console.service.impl.MetaGenerator;
import com.ctrip.framework.drc.console.service.v2.MetaCompareService;
import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV2;
import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV3;
import com.ctrip.framework.drc.console.utils.XmlUtils;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StopWatch;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/drc/v2/compare/")
public class DrcCompareController {
    private static final Logger logger = LoggerFactory.getLogger(DrcCompareController.class);

    @Autowired
    private MetaGenerator metaGenerator;
    @Autowired
    private MetaGeneratorV2 metaGeneratorV2;
    @Autowired
    private MetaGeneratorV3 metaGeneratorV3;
    @Autowired
    private MetaCompareService metaCompareService;

    @GetMapping("doBenchMark")
    public ApiResult<GeneratorStatistics.Task> benchMarkTest(@RequestParam("compare") Boolean compare) {
        try {
            List<GeneratorStatistics.Task> list = Lists.newArrayList();
            StopWatch stopWatch = new StopWatch();

            // v3
            logger.info("start v3");
            stopWatch.start(metaGeneratorV3.getClass().getSimpleName());
            Drc drcV3 = metaGeneratorV3.getDrc();
            stopWatch.stop();
            list.add(new GeneratorStatistics.Task(stopWatch.getLastTaskName(), stopWatch.getLastTaskTimeMillis()));


            // v1
            logger.info("start v1");
            stopWatch.start(metaGenerator.getClass().getSimpleName());
            metaGenerator.getDrc();
            stopWatch.stop();
            list.add(new GeneratorStatistics.Task(stopWatch.getLastTaskName(), stopWatch.getLastTaskTimeMillis()));


            // v2
            logger.info("start v2");
            stopWatch.start(metaGeneratorV2.getClass().getSimpleName());
            Drc drcV2 = metaGeneratorV2.getDrc();
            stopWatch.stop();
            list.add(new GeneratorStatistics.Task(stopWatch.getLastTaskName(), stopWatch.getLastTaskTimeMillis()));


            // compareResult
            String summaryInfo = stopWatch.prettyPrint();
            Boolean compareResult = null;
            if (Boolean.TRUE.equals(compare)) {
                stopWatch.start(metaCompareService.getClass().getSimpleName());
                String res = metaCompareService.compareDrcMeta(drcV2, drcV3);
                stopWatch.stop();
                list.add(new GeneratorStatistics.Task(stopWatch.getLastTaskName(), stopWatch.getLastTaskTimeMillis()));

                if (!StringUtils.isEmpty(res)) {
                    summaryInfo += String.format("\n-----Compare Result------\n\n%s\n\n--------------------\n", res);
                }
                compareResult = metaCompareService.isConsistent(res);
            }
            logger.info(summaryInfo);
            return ApiResult.getSuccessInstance(new GeneratorStatistics(list, compareResult, summaryInfo));
        } catch (Throwable e) {
            logger.error("benchMarkTest error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }


    @GetMapping("generateV3")
    public ApiResult<String> generateV3() {
        try {
            // v3
            logger.info("start v3");
            Drc drcV3 = metaGeneratorV3.getDrc();
            logger.info("finish v3");
            return ApiResult.getSuccessInstance(XmlUtils.formatXML(drcV3.toString()));
        } catch (Throwable e) {
            logger.error("benchMarkTest error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("generateV2")
    public ApiResult<String> generateV2() {
        try {
            // v3
            logger.info("start v2");
            Drc drcV3 = metaGeneratorV2.getDrc();
            logger.info("finish v2");
            return ApiResult.getSuccessInstance(XmlUtils.formatXML(drcV3.toString()));
        } catch (Throwable e) {
            logger.error("benchMarkTest error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }


    static class GeneratorStatistics {
        private final List<Task> task;
        private final Boolean same;
        private final String summaryInfo;

        public GeneratorStatistics(List<Task> task, Boolean same, String summaryInfo) {
            this.task = task;
            this.same = same;
            this.summaryInfo = summaryInfo;
        }

        public List<Task> getTask() {
            return task;
        }

        public String getSummaryInfo() {
            return summaryInfo;
        }

        public Boolean getSame() {
            return same;
        }

        public static class Task {
            private final String name;
            private final Long seconds;


            public Task(String name, Long seconds) {
                this.name = name;
                this.seconds = seconds;
            }

            public String getName() {
                return name;
            }

            public Long getSeconds() {
                return seconds;
            }

        }

    }


}
