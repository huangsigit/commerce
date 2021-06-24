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
public interface CostMapper {

    List<Map<String, Object>> selectCost(Map map);

    int selectCostCount(Map map);

    Map<String, Object> selectCostByMonth(@Param("month") String month);

    Map<String, Object> selectCostByMonthAndItemId(@Param("month") String month, @Param("item_id") Long itemId);


    int insertCost(Map map);

    void updateCost(Map map);

    int deleteByMonth(@Param("month") String month);

    int deleteCostById(@Param("id") long id);

    List<Map<String, Object>> selectOverall(Map map);

    Map<String, Object> selectCostByMonthAndItemsName(@Param("month") String month, @Param("itemsName") String itemsName);

}
