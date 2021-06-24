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
public interface UserItemService {

    List<Map<String, Object>> selectUserItem(Map map);

    int selectUserItemCount(Map map);

    List<Map<String, Object>> selectUserItemByUserId(Integer userId);

    boolean insertUserItem(Map map);

    void updateUserItem(Map map);

    boolean deleteUserItemById(Integer id);

    boolean deleteUserItemByUserId(Integer userId);


}
