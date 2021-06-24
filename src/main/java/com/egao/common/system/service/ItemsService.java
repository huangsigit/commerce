package com.egao.common.system.service;

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
public interface ItemsService {

    List<Map<String, Object>> selectItems(Map map);

    int selectItemsCount(Map map);

    List<Map<String, Object>> selectItemsByJobNumber(Map map);

    Map<String, Object> selectItemsById(String id);

    Map<String, Object> selectItemsByName(String name);


    void insertItems(Map map);

    void updateItems(Map map);


    Map<String, Object> selectJobNumberById(String id);

    List<Map<String, Object>> selectJobNumberByItemsId(String itemsId);

    void insertJobNumber(Map map);

    void deleteAllItems();

    void deleteByType(Integer type);

    void deleteByBusinessId(Long businessId, Integer type);

    void syncGoogleItemsData();

    void syncFacebookItemsData();

    List<Map<String, Object>> selectItemsByType(Integer type);

    List<Map<String, Object>> selectItemsByType2(Integer type);

    List<Map<String, Object>> selectAllItemsByType(Integer type);

    List<Map<String, Object>> selectItemsByUserIdAndType(Integer userId, Integer type);

    void updateBindingStatusById(Long id, Integer bindingStatus);


    List<Map<String, Object>> selectItem(Map map);

    int selectItemCount(Map map);

    List<Map<String, Object>> selectItemByParentId(Long parentId);

    List<Map<String, Object>> selectItemsCanBind(Integer type);

    Long insertItem(Map map);

    void updateItem(Map map);

    void deleteItemById(Long id);

    void deleteItemByParentId(Long id);

    Map<String, Object> selectItemById(Long id);

    List<Map<String, Object>> selectAdAccountByItemId(Long itemId, Integer accountType);

    List<Map<String, Object>> selectFBAdAccountByItemId(Long itemId, Integer accountType);

    List<Map<String, Object>> selectItemCost(Map map);

    List<Map<String, Object>> selectItemData(Map map);

    List<Map<String, Object>> selectChartData(Map map);

}
