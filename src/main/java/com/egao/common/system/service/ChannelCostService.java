package com.egao.common.system.service;

import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * 菜单表 服务类
 * </p>
 *
 * @author hs
 * @since 2019-10-10
 */
public interface ChannelCostService {

    List<Map<String, Object>> selectChannelCost(Map map);

    int selectChannelCostCount(Map map);

    Map<String, Object> selectChannelCostByChannelName(String channelName);

    Map<String, Object> selectChannelCostByItemIdAndChannelId(String date, Long item_id, Long channel_id);

    boolean insertChannelCost(Map map);

    void updateChannelCost(Map map);


    boolean deleteChannelCostById(Integer id);

    Float selectRevenue(Map map);




}
