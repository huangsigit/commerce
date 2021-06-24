package com.egao.common.system.service;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * 渠道表
 * </p>
 *
 * @author hs
 * @since 2019-10-10
 */
public interface ChannelService {

    List<Map<String, Object>> selectChannel(Map map);

    int selectChannelCount(Map map);

    List<Map<String, Object>> selectChannelList(Map map);


    List<Map<String, Object>> selectCanBindChannel(Map map);

    Map<String, Object> selectChannelById(Integer id);

    List<Map<String, Object>> selectChannelByParentId(Integer parentid);


    int insertChannel(Map map);

    void updateChannel(Map map);

    void deleteById(Integer id);

    void deleteByParentId(Integer parentId);

    List<Map<String, Object>> selectChannelData(Map map);

//    List<Map<String, Object>> selectChannelData2(Map map);

    int selectChannelDataCount(Map map);

    List<Map<String, Object>> selectChannelCost(Map map);

    List<Map<String, Object>> selectChannelRevenue(Map map);

    Map<String, Object> gatherChannelData(List<Map<String, Object>> channelList, Map map);


}
