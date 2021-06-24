package com.egao.common.system.mapper;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * 渠道表 Mapper 接口
 * </p>
 *
 * @author hs
 * @since 2020-11-11
 */
@Component
public interface ChannelMapper {

    List<Map<String, Object>> selectChannel(Map map);

    int selectChannelCount(Map map);

    List<Map<String, Object>> selectChannelList(Map map);


    List<Map<String, Object>> selectCanBindChannel(Map map);

    Map<String, Object> selectChannelById(@Param("id") Integer id);

    List<Map<String, Object>> selectChannelByParentId(@Param("parentId") Integer parentId);


    int insertChannel(Map map);

    void updateChannel(Map map);

    void deleteById(@Param("id") Integer id);

    void deleteByParentId(@Param("parentId") Integer parentId);

    List<Map<String, Object>> selectChannelData(Map map);

    int selectChannelDataCount(Map map);

    List<Map<String, Object>> selectChannelCost(Map map);

    List<Map<String, Object>> selectChannelRevenue(Map map);

}
