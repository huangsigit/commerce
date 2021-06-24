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
public interface BusinessService {

    List<Map<String, Object>> selectBusiness(Map map);

    int selectBusinessCount(Map map);

    Map<String, Object> selectBusinessById(Long id);

    Map<String, Object> selectBusinessByBusinessId(String id);

    void insertBusiness(Map map);

    void updateBusiness(Map map);

    boolean deleteById(long id);


}
