package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.v2.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v2.MhaReplicationDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.TransmissionTypeEnum;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaReplicationServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.StreamUtils;
import com.ctrip.framework.drc.console.vo.display.v2.DelayInfoVo;
import com.ctrip.framework.drc.console.vo.display.v2.MhaReplicationVo;
import com.ctrip.framework.drc.console.vo.display.v2.MhaVo;
import com.ctrip.framework.drc.console.vo.request.MhaQueryDto;
import com.ctrip.framework.drc.console.vo.request.MhaReplicationQueryDto;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.PageResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;


@RestController
@RequestMapping("/api/drc/v2/replication/")
public class MhaReplicationController {
    private static final Logger logger = LoggerFactory.getLogger(MhaReplicationController.class);

    @Autowired
    private MhaReplicationServiceV2 mhaReplicationServiceV2;

    @Autowired
    private MhaServiceV2 mhaServiceV2;

    @Autowired
    private MetaInfoServiceV2 metaInfoServiceV2;


    @GetMapping("queryMhaRelated")
    @SuppressWarnings("unchecked")
    public ApiResult<List<MhaReplicationVo>> queryMhaReplications(@RequestParam(name = "relatedMhaId") List<Long> relatedMhaId) {
        logger.info("[meta] queryMhaReplications:{}", relatedMhaId);
        try {
            if (CollectionUtils.isEmpty(relatedMhaId)) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "Invalid input, contact devops!");
            }

            // query related replications
            List<MhaReplicationTbl> replicationTbls = mhaReplicationServiceV2.queryRelatedReplications(relatedMhaId);
            replicationTbls = replicationTbls.stream().filter(e -> e.getDrcStatus().equals(BooleanEnum.TRUE.getCode())).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(replicationTbls)) {
                return ApiResult.getSuccessInstance(Collections.emptyList());
            }

            // query mha detail
            Set<Long> mhaIdSet = Sets.newHashSet();
            mhaIdSet.addAll(replicationTbls.stream().map(MhaReplicationTbl::getSrcMhaId).collect(Collectors.toSet()));
            mhaIdSet.addAll(replicationTbls.stream().map(MhaReplicationTbl::getDstMhaId).collect(Collectors.toSet()));
            Map<Long, MhaTblV2> mhaTblMap = mhaServiceV2.queryMhaByIds(Lists.newArrayList(mhaIdSet));

            // fill
            return ApiResult.getSuccessInstance(this.buildVo(replicationTbls, mhaTblMap));
        } catch (Exception e) {
            logger.error("queryMhaReplications error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("query")
    @SuppressWarnings("unchecked")
    public ApiResult<PageResult<MhaReplicationVo>> queryMhaReplicationsByPage(MhaReplicationQueryDto queryDto) {
        logger.info("[meta] get allOrderedGroup,drcGroupQueryDto:{}", queryDto.toString());
        try {
            MhaReplicationQuery query = new MhaReplicationQuery();
            query.setPageIndex(queryDto.getPageIndex());
            query.setPageSize(queryDto.getPageSize());

            // convert query condition
            MhaQueryDto srcMha = queryDto.getSrcMha();
            if (srcMha != null && srcMha.isConditionalQuery()) {
                Map<Long, MhaTblV2> mhaTblV2Map = mhaServiceV2.query(StringUtils.trim(srcMha.getName()), srcMha.getBuId(), srcMha.getRegionId());
                if (CollectionUtils.isEmpty(mhaTblV2Map)) {
                    return ApiResult.getSuccessInstance(PageResult.emptyResult());
                }
                query.setSrcMhaIdList(Lists.newArrayList(mhaTblV2Map.keySet()));
            }
            MhaQueryDto dstMha = queryDto.getDstMha();
            if (dstMha != null && dstMha.isConditionalQuery()) {
                Map<Long, MhaTblV2> mhaTblV2Map = mhaServiceV2.query(StringUtils.trim(dstMha.getName()), dstMha.getBuId(), dstMha.getRegionId());
                if (CollectionUtils.isEmpty(mhaTblV2Map)) {
                    return ApiResult.getSuccessInstance(PageResult.emptyResult());
                }
                query.setDstMhaIdList(Lists.newArrayList(mhaTblV2Map.keySet()));
            }

            // query replication
            PageResult<MhaReplicationTbl> tblPageResult = mhaReplicationServiceV2.queryByPage(query);
            List<MhaReplicationTbl> data = tblPageResult.getData();
            if (tblPageResult.getTotalCount() == 0) {
                return ApiResult.getSuccessInstance(PageResult.emptyResult());
            }

            // query mha detail information
            Set<Long> mhaIdSet = Sets.newHashSet();
            mhaIdSet.addAll(data.stream().map(MhaReplicationTbl::getSrcMhaId).collect(Collectors.toSet()));
            mhaIdSet.addAll(data.stream().map(MhaReplicationTbl::getDstMhaId).collect(Collectors.toSet()));
            Map<Long, MhaTblV2> mhaTblMap = mhaServiceV2.queryMhaByIds(Lists.newArrayList(mhaIdSet));

            List<MhaReplicationVo> res = this.buildVo(data, mhaTblMap);

            // simplex or duplex
            List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationServiceV2.queryRelatedReplications(Lists.newArrayList(mhaIdSet));
            Set<String> links = mhaReplicationTbls.stream()
                    .filter(e -> BooleanEnum.TRUE.getCode().equals(e.getDrcStatus()))
                    .map(e -> e.getSrcMhaId() + "->" + e.getDstMhaId()).collect(Collectors.toSet());
            res.forEach(e -> {
                boolean hasLink = BooleanEnum.TRUE.getCode().equals(e.getStatus());
                boolean hasReverseLink = links.contains(e.getDstMha().getId() + "->" + e.getSrcMha().getId());
                TransmissionTypeEnum type = TransmissionTypeEnum.NOCONFIG;
                if (hasLink && hasReverseLink) {
                    type = TransmissionTypeEnum.DUPLEX;
                } else if (hasLink || hasReverseLink) {
                    type = TransmissionTypeEnum.SIMPLEX;
                }
                e.setType(type.getType());
            });

            return ApiResult.getSuccessInstance(
                    PageResult.newInstance(res, tblPageResult.getPageIndex(), tblPageResult.getPageSize(), tblPageResult.getTotalCount())
            );
        } catch (Exception e) {
            logger.error("queryMhaReplicationsByPage error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }


    /**
     * @return src -> dst delay
     */
    @GetMapping("delay")
    @SuppressWarnings("unchecked")
    public ApiResult<DelayInfoVo> getMhaReplicationDelay(@RequestParam String srcMha, @RequestParam String dstMha) {
        logger.info("getMhaDelay: {} -> {}", srcMha, dstMha);
        try {
            if (StringUtils.isBlank(srcMha) || StringUtils.isBlank(dstMha)) {
                return ApiResult.getFailInstance(null, "mha name should not be blank!");
            }

            MhaDelayInfoDto mhaReplicationDelay = mhaReplicationServiceV2.getMhaReplicationDelay(srcMha.trim(), dstMha.trim());
            return ApiResult.getSuccessInstance(DelayInfoVo.from(mhaReplicationDelay));
        } catch (Throwable e) {
            logger.error(String.format("getMhaDelay error: %s->%s", srcMha, dstMha), e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("relatedReplicationDelay")
    @SuppressWarnings("unchecked")
    public ApiResult<List<MhaReplicationDto>> queryDetail(@RequestParam(name = "mha1") String mha1,
                                                    @RequestParam(name = "mha2") String mha2,
                                                    @RequestParam(name = "dbs") List<String> dbs) {
        if (StringUtils.isBlank(mha1) || StringUtils.isBlank(mha2) || CollectionUtils.isEmpty(dbs)) {
            return ApiResult.getSuccessInstance(Collections.emptyList());
        }
        try {
            List<MhaReplicationDto> res = Lists.newArrayList();
            res.addAll(mhaReplicationServiceV2.queryRelatedReplications(mha1, dbs));
            res.addAll(mhaReplicationServiceV2.queryRelatedReplications(mha2, dbs));
            res = res.stream().filter(StreamUtils.distinctByKey(MhaReplicationDto::getReplicationId)).collect(Collectors.toList());
            List<MhaDelayInfoDto> mhaReplicationDelays = mhaReplicationServiceV2.getMhaReplicationDelays(res);
            Map<String, MhaDelayInfoDto> delayMap = mhaReplicationDelays.stream().filter(e -> e.getDelay() != null).collect(Collectors.toMap(
                    e -> e.getSrcMha() + "-" + e.getDstMha(),
                    Function.identity(),
                    (e1, e2) -> e1)
            );
            res.forEach(e -> {
                String key = e.getSrcMha().getName() + "-" + e.getDstMha().getName();
                e.setDelayInfoDto(delayMap.get(key));
            });
            return ApiResult.getSuccessInstance(res);

        } catch (Throwable e) {
            logger.error("queryByPage error", e);
            throw e;
//            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }


    private List<MhaReplicationVo> buildVo(List<MhaReplicationTbl> replicationTblList, Map<Long, MhaTblV2> mhaTblMap) {
        // prepare meta data
        List<DcDo> dcDos = metaInfoServiceV2.queryAllDcWithCache();
        List<BuTbl> buTbls = metaInfoServiceV2.queryAllBuWithCache();

        Map<Long, DcDo> dcMap = dcDos.stream().collect(Collectors.toMap(DcDo::getDcId, Function.identity()));
        Map<Long, BuTbl> buMap = buTbls.stream().collect(Collectors.toMap(BuTbl::getId, Function.identity()));

        return replicationTblList.stream().map(replicationTbl -> {
            MhaReplicationVo vo = new MhaReplicationVo();
            MhaTblV2 srcMhaTbl = mhaTblMap.get(replicationTbl.getSrcMhaId());
            MhaTblV2 dstMhaTbl = mhaTblMap.get(replicationTbl.getDstMhaId());

            // pre-check data integrity
            if (srcMhaTbl == null) {
                throw ConsoleExceptionUtils.message(
                        ReadableErrorDefEnum.QUERY_DATA_INCOMPLETE,
                        String.format("srcMhaTbl not exist. replicationTbl:%s, srcMhaTblId:%d", replicationTbl, replicationTbl.getSrcMhaId())
                );
            }
            if (dstMhaTbl == null) {
                throw ConsoleExceptionUtils.message(
                        ReadableErrorDefEnum.QUERY_DATA_INCOMPLETE,
                        String.format("dstMhaTbl not exist. replicationTbl:%s, dstMhaTblId:%d", replicationTbl, replicationTbl.getDstMhaId())
                );
            }
            // set vo: mha
            vo.setSrcMha(MhaVo.from(srcMhaTbl, dcMap.get(srcMhaTbl.getDcId()), buMap.get(srcMhaTbl.getBuId())));
            vo.setDstMha(MhaVo.from(dstMhaTbl, dcMap.get(dstMhaTbl.getDcId()), buMap.get(dstMhaTbl.getBuId())));
            vo.setStatus(replicationTbl.getDrcStatus());

            // set vo: replication
            vo.setReplicationId(String.valueOf(replicationTbl.getId()));
            if (replicationTbl.getDatachangeLasttime() != null) {
                vo.setDatachangeLasttime(replicationTbl.getDatachangeLasttime().getTime());
            }
            return vo;
        }).collect(Collectors.toList());
    }
}
