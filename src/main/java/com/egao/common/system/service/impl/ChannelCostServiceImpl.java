package com.egao.common.system.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.egao.common.system.mapper.ChannelCostMapper;
import com.egao.common.system.mapper.ChannelMapper;
import com.egao.common.system.mapper.ItemsMapper;
import com.egao.common.system.service.ChannelCostService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
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
public class ChannelCostServiceImpl implements ChannelCostService {

    private Logger logger = LoggerFactory.getLogger("ChannelCostServiceImpl");

    @Autowired
    public ChannelCostMapper costMapper;

    @Autowired
    public ItemsMapper itemsMapper;

    @Autowired
    public ChannelMapper channelMapper;


    public List<Map<String, Object>> selectChannelCost(Map map){
        List<Map<String, Object>> costList = costMapper.selectChannelCost(map);

        return costList;
    }

    public int selectChannelCostCount(Map map){
        int costCount = costMapper.selectChannelCostCount(map);
        return costCount;
    }

    public Map<String, Object> selectChannelCostByChannelName(String channelName){
        Map<String, Object> costMap = costMapper.selectChannelCostByChannelName(channelName);
        return costMap;
    }

    public Map<String, Object> selectChannelCostByItemIdAndChannelId(String dates, Long itemId, Long channelId){
        Map<String, Object> costMap = costMapper.selectChannelCostByItemIdAndChannelId(dates, itemId, channelId);
        return costMap;
    }

    public static boolean isAuth(List<Map<String, Object>> userItemList, Integer itemsId, Integer userId){
        for(Map<String, Object> userItemMap : userItemList){
            Integer user_id = (Integer)userItemMap.get("user_id");
            Integer item_id = (Integer)userItemMap.get("item_id");
            if(itemsId.equals(item_id) && userId.equals(user_id)){
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean insertChannelCost(Map map){
//        dates, item_id, channel_id, cost
        String dates = (String)map.get("dates");
        Long itemsId = (Long)map.get("item_id");
        Long channelId = (Long)map.get("channel_id");
        List userItemList = (List)map.get("userItemList");
        Integer userId = (Integer)map.get("userId");



        // 站点一定要存在
        Map<String, Object> itemsMap = itemsMapper.selectItemsById(String.valueOf(itemsId));
        if(itemsMap == null){
            System.out.println("站点不存在："+map);
            return false;
        }

        System.out.println("userItemList："+userItemList);
        System.out.println("itemsId："+itemsId);
        System.out.println("userId："+userId);

        boolean isAuth = isAuth(userItemList, Integer.valueOf(String.valueOf(itemsId)), userId);
        System.out.println("insertChannelCost isAuth："+ JSONArray.toJSONString(isAuth));
        if (isAuth == false) {
            System.out.println("无站点权限："+map);
            return false;
        }

        Map<String, Object> channelMap = channelMapper.selectChannelById(Integer.valueOf(String.valueOf(channelId)));
        if(channelMap == null){
            System.out.println("渠道不存在："+map);
            return false;
        }

        Map<String, Object> costMap = costMapper.selectChannelCostByItemIdAndChannelId(dates, itemsId, Long.valueOf(channelId));
        if(costMap != null){
            System.out.println("站点和渠道已存在："+map);
            return false;
        }

        int count = costMapper.insertChannelCost(map);
        if(count > 0){
            System.out.println("渠道成本插入成本");
            return true;
        }

        return false;
    }


    @Override
    public void updateChannelCost(Map map){
        costMapper.updateChannelCost(map);
    }


    @Override
    public boolean deleteChannelCostById(Integer id){
        int count = costMapper.deleteChannelCostById(id);
        return count > 0 ? true : false;
    }

    public Float selectRevenue(Map map){
        Float count = costMapper.selectRevenue(map);
        return count;

    }


}
