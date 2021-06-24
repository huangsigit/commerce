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
public interface ItemsMapper {

    List<Map<String, Object>> selectItems(Map map);

    int selectItemsCount(Map map);

    List<Map<String, Object>> selectItemsByJobNumber(Map map);

    Map<String, Object> selectItemsById(@Param("id") String id);

    Map<String, Object> selectItemsByName(@Param("name") String name);

    void insertItems(Map map);

    void updateItems(Map map);


    Map<String, Object> selectJobNumberById(@Param("id") String id);

    List<Map<String, Object>> selectJobNumberByItemsId(@Param("itemsId") String itemsId);

    List<Map<String, Object>> selectItemsCanBind(@Param("type") Integer type);


    void insertJobNumber(Map map);

    void deleteAllItems();

    void deleteByType(@Param("type") Integer type);

    void deleteByBusinessId(@Param("business_id") Long businessId, @Param("type") Integer type);

    List<Map<String, Object>> selectItemsByType(@Param("type") Integer type);

    List<Map<String, Object>> selectItemsByType2(@Param("type") Integer type);

    List<Map<String, Object>> selectAllItemsByType(@Param("type") Integer type);


    List<Map<String, Object>> selectItemsByUserIdAndType(@Param("userId") Integer userId, @Param("type") Integer type);


    void updateBindingStatusById(@Param("id") Long id, @Param("bindingStatus") Integer bindingStatus);



    List<Map<String, Object>> selectItem(Map map);

    int selectItemCount(Map map);

    List<Map<String, Object>> selectItemByParentId(@Param("parentId")Long parentId);


    int insertItem(Map map);

    void updateItem(Map map);

    void deleteItemById(@Param("id") Long id);

    void deleteItemByParentId(@Param("parentId") Long parentId);

    Map<String, Object> selectItemById(@Param("id") Long id);

    List<Map<String, Object>> selectAdAccountByItemId(@Param("itemId") Long itemId, @Param("accountType") Integer accountType);

    List<Map<String, Object>> selectFBAdAccountByItemId(@Param("itemId") Long itemId, @Param("accountType") Integer accountType);

    List<Map<String, Object>> selectItemCost(Map map);

    List<Map<String, Object>> selectChannelRevenue(Map map);

    List<Map<String, Object>> selectItemData(Map map);

    List<Map<String, Object>> selectChartData(Map map);

}
