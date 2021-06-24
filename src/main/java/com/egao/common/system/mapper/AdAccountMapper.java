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
public interface AdAccountMapper {

    List<Map<String, Object>> selectAdAccountByType(@Param("type") Integer type);

    List<Map<String, Object>> selectAdAccountByItemsId(@Param("items_id") Long itemsId);

    Map<String, Object> selectAdAccountById(@Param("id") Long id);

    void insertAdAccount(Map map);

    void deleteById(@Param("id") long id);



}
