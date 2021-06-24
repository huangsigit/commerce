package com.egao.common.system.mapper;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * 菜单表 Mapper 接口
 * </p>
 *
 * @author hs
 * @since 2020-11-11
 */
@Component
public interface ChannelCostMapper {

    List<Map<String, Object>> selectChannelCost(Map map);

    int selectChannelCostCount(Map map);

    Map<String, Object> selectChannelCostByChannelName(@Param("channelName") String channelName);

    Map<String, Object> selectChannelCostByItemIdAndChannelId(@Param("dates") String dates, @Param("item_id") Long itemId, @Param("channel_id") Long channelId);


    int insertChannelCost(Map map);

    void updateChannelCost(Map map);


    int deleteChannelCostById(@Param("id") Integer id);

    Float selectRevenue(Map map);


}
