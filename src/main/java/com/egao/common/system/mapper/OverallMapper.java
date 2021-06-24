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
public interface OverallMapper {

    List<Map<String, Object>> selectOverall(Map map);

    int selectOverallCount(Map map);

    void insertOverall(Map map);

    void updateOverall(Map map);

    int deleteById(@Param("id") long id);



}
