package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.console.service.v2.MhaReplicationServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.vo.display.v2.MhaGroupPairVo;
import com.ctrip.framework.drc.console.vo.request.DrcGroupQueryDto;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.PageResult;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@RestController
@RequestMapping("/api/drc/v2/replication/")
public class MhaReplicationController {
    private static final Logger logger = LoggerFactory.getLogger(MhaReplicationController.class);

    @Autowired
    private MhaReplicationServiceV2 mhaReplicationServiceV2;

    @Autowired
    private MhaServiceV2 mhaServiceV2;


    @GetMapping("query")
    public ApiResult<PageResult<MhaGroupPairVo>> listOrderedGroupsByPage(@RequestBody DrcGroupQueryDto queryDto) {
        logger.info("[meta] get allOrderedGroup,drcGroupQueryDto:{}", queryDto);
        try {
            MhaReplicationQuery query = new MhaReplicationQuery();
            query.setPageIndex(queryDto.getPageIndex());
            query.setPageSize(queryDto.getPageSize());

            // filter condition 1：mha names
            if (StringUtils.isNotBlank(queryDto.getSrcMha())) {
                List<Long> srcMhaIds = this.queryMhaIdsByNames(queryDto.getSrcMha());
                query.setSrcMhaIdList(srcMhaIds);
            }
            if (StringUtils.isNotBlank(queryDto.getDestMha())) {
                List<Long> destMhaIds = this.queryMhaIdsByNames(queryDto.getDestMha());
                query.setDesMhaIdList(destMhaIds);
            }

            // query replication
            PageResult<MhaReplicationTbl> tblPageResult = mhaReplicationServiceV2.queryByPage(query);
            List<MhaReplicationTbl> data = tblPageResult.getData();
            if (CollectionUtils.isEmpty(data)) {
                return ApiResult.getFailInstance(null, "empty result!");
            }

            // query mha detail information
            List<Long> mhaIds = Lists.newArrayList();
            mhaIds.addAll(data.stream().map(MhaReplicationTbl::getSrcMhaId).collect(Collectors.toList()));
            mhaIds.addAll(data.stream().map(MhaReplicationTbl::getDstMhaId).collect(Collectors.toList()));
            Map<Long, MhaTblV2> mhaTblMap = mhaServiceV2.queryMhaByIds(mhaIds);

            List<MhaGroupPairVo> res = this.buildVo(data, mhaTblMap);
            return ApiResult.getSuccessInstance(
                    PageResult.newInstance(res, tblPageResult.getPageIndex(), tblPageResult.getPageSize(), tblPageResult.getTotalCount())
            );
        } catch (Exception e) {
            logger.error("[meta] get MhaGroup error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    private List<MhaGroupPairVo> buildVo(List<MhaReplicationTbl> replicationTblList, Map<Long, MhaTblV2> mhaTblMap) {
        return replicationTblList.stream().map(replicationTbl -> {
            MhaGroupPairVo vo = new MhaGroupPairVo();
            MhaTblV2 srcMhaTbl = mhaTblMap.get(replicationTbl.getSrcMhaId());
            MhaTblV2 dstMhaTbl = mhaTblMap.get(replicationTbl.getDstMhaId());
            if(srcMhaTbl == null ){
                throw ConsoleExceptionUtils.message(String.format("渲染失败。srcMhaTbl 不存在. replicationTbl:%s, srcMhaTblId:%d", replicationTbl, replicationTbl.getSrcMhaId()));
            }
            if(dstMhaTbl == null ){
                throw ConsoleExceptionUtils.message(String.format("渲染失败。dstMhaTbl 不存在. replicationTbl:%s, dstMhaTblId:%d", replicationTbl, replicationTbl.getDstMhaId()));
            }
            vo.setSrcMha(srcMhaTbl.getMhaName());
            vo.setDstMha(dstMhaTbl.getMhaName());
            vo.setBuId(srcMhaTbl.getBuId());
            vo.setSrcMhaMonitorSwitch(srcMhaTbl.getMonitorSwitch());
            vo.setDstMhaMonitorSwitch(dstMhaTbl.getMonitorSwitch());
            return vo;
        }).collect(Collectors.toList());
    }

    /**
     *
     * @param mhaNames mha names joined by ','. e.g. "mha1,mha2"
     * @return corresponding mha id list
     */
    private List<Long> queryMhaIdsByNames(String mhaNames) {
        List<String> srcMhaNameList = Lists.newArrayList(mhaNames.trim().split(",")).stream().distinct().collect(Collectors.toList());
        Map<String, MhaTblV2> mhaTblV2Map = mhaServiceV2.queryMhaByNames(srcMhaNameList);
        return mhaTblV2Map.values().stream().map(MhaTblV2::getId).collect(Collectors.toList());
    }

}
