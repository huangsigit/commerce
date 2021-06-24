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
public interface AdAccountService {

    List<Map<String, Object>> selectAdAccountByType(Integer type);

    List<Map<String, Object>> selectAdAccountByItemsId(Long itemsId);

    Map<String, Object> selectAdAccountById(Long id);

    void insertAdAccount(Map map);

    void deleteById(long id);


}
