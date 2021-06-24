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
public interface UserItemMapper {

    List<Map<String, Object>> selectUserItem(Map map);

    int selectUserItemCount(Map map);

    List<Map<String, Object>> selectUserItemByUserId(@Param("userId") Integer userId);

    int insertUserItem(Map map);

    void updateUserItem(Map map);

    int deleteUserItemById(@Param("id") Integer id);

    int deleteUserItemByUserId(@Param("userId") Integer userId);


}
