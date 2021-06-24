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
public interface OverallService {

    List<Map<String, Object>> selectOverall(Map map);

    int selectOverallCount(Map map);

    void insertOverall(Map map);

    void updateOverall(Map map);

    boolean deleteById(long id);


}
