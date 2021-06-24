package com.egao.common.system.service.impl;

import com.egao.common.system.mapper.OverallMapper;
import com.egao.common.system.mapper.OverallMapper;
import com.egao.common.system.service.OverallService;
import com.egao.common.system.service.OverallService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * 菜单表 服务实现类
 * </p>
 *
 * @author hs
 * @since 2020-10-10
 */
@Service
public class OverallServiceImpl implements OverallService {

    private Logger logger = LoggerFactory.getLogger("OverallServiceImpl");

    @Autowired
    public OverallMapper overallMapper;

    public List<Map<String, Object>> selectOverall(Map map){
        List<Map<String, Object>> overallList = overallMapper.selectOverall(map);

        return overallList;
    }

    public int selectOverallCount(Map map){
        int overallCount = overallMapper.selectOverallCount(map);
        return overallCount;
    }

    @Override
    public void insertOverall(Map map){
        overallMapper.insertOverall(map);
    }

    @Override
    public void updateOverall(Map map){
        overallMapper.updateOverall(map);
    }

    @Override
    public boolean deleteById(long id){
        int count = overallMapper.deleteById(id);
        return count > 0 ? true : false;
    }


}
