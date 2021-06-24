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
public interface CostService {

    List<Map<String, Object>> selectCost(Map map);

    int selectCostCount(Map map);

    Map<String, Object> selectCostByMonth(String month);

    Map<String, Object> selectCostByMonthAndItemId(String month, Long item_id);


    boolean insertCost(Map map);

    void updateCost(Map map);

    boolean deleteByMonth(String month);

    boolean deleteCostById(long id);

    List<Map<String, Object>> selectOverall(Map map);

    Map<String, Object> selectCostByMonthAndItemsName(String month, String itemsName);

}
