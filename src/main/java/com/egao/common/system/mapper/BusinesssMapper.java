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
public interface BusinesssMapper {

    List<Map<String, Object>> selectBusiness(Map map);

    int selectBusinessCount(Map map);

    Map<String, Object> selectBusinessById(Long id);

    Map<String, Object> selectBusinessByBusinessId(String id);

    void insertBusiness(Map map);

    void updateBusiness(Map map);

    int deleteById(@Param("id") long id);


}
