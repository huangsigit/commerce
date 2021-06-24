package com.egao.common.system.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.AdConstant;
import com.egao.common.core.UploadConstant;
import com.egao.common.core.utils.AnalyticsUtil;
import com.egao.common.core.utils.HttpUtil;
import com.egao.common.system.mapper.CostMapper;
import com.egao.common.system.mapper.ItemsMapper;
import com.egao.common.system.service.CertificateService;
import com.egao.common.system.service.CostService;
import com.egao.common.system.service.CostService;
import com.google.api.services.analytics.model.AccountSummary;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.Param;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * 菜单表 服务实现类
 * </p>
 *
 * @author hs
 * @since 2020-10-10
 */
@Service
public class CostServiceImpl implements CostService {

    private Logger logger = LoggerFactory.getLogger("CostServiceImpl");

    @Autowired
    public CostMapper costMapper;

    @Autowired
    public ItemsMapper itemsMapper;

    public List<Map<String, Object>> selectCost(Map map){
        List<Map<String, Object>> costList = costMapper.selectCost(map);

        return costList;
    }

    public int selectCostCount(Map map){
        int costCount = costMapper.selectCostCount(map);
        return costCount;
    }

    public Map<String, Object> selectCostByMonth(String month){
        Map<String, Object> costMap = costMapper.selectCostByMonth(month);
        return costMap;
    }

    public Map<String, Object> selectCostByMonthAndItemId(String month, Long itemId){
        Map<String, Object> costMap = costMapper.selectCostByMonthAndItemId(month, itemId);
        return costMap;
    }


    @Override
    public boolean insertCost(Map map){
        String month = (String)map.get("month");
        Long itemsId = (Long)map.get("item_id");

        if(itemsId == null){
            System.out.println("站点ID不存在");
            return false;
        }

        // 站点一定要存在
        Map<String, Object> itemsMap = itemsMapper.selectItemsById(String.valueOf(itemsId));
        if(itemsMap == null){
            System.out.println("站点一定要存在");
            return false;
        }

        // 月份和站点不能重复
        Map<String, Object> costMap = selectCostByMonthAndItemId(month, itemsId);
        if(costMap != null){
            System.out.println("月份和站点不能重复："+costMap);
            return false;
        }

        int count = costMapper.insertCost(map);
        if(count > 0){
            return true;
        }

        return false;
    }


    @Override
    public void updateCost(Map map){

        costMapper.updateCost(map);
    }

    @Override
    public boolean deleteByMonth(String month){
        int count = costMapper.deleteByMonth(month);
        return count > 0 ? true : false;
    }

    @Override
    public boolean deleteCostById(long id){
        int count = costMapper.deleteCostById(id);
        return count > 0 ? true : false;
    }


    public List<Map<String, Object>> selectOverall(Map map){
        List<Map<String, Object>> overallList = costMapper.selectOverall(map);
        return overallList;
    }

    public Map<String, Object> selectCostByMonthAndItemsName(String month, String itemsName){
        Map<String, Object> costMap = costMapper.selectCostByMonthAndItemsName(month, itemsName);
        return costMap;
    }



}
