package com.egao.common.system.service.impl;

import com.egao.common.system.mapper.BusinessMapper;
import com.egao.common.system.mapper.BusinesssMapper;
import com.egao.common.system.service.BusinessService;
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
public class BusinessServiceImpl implements BusinessService {

    private Logger logger = LoggerFactory.getLogger("BusinessServiceImpl");

    @Autowired
    public BusinesssMapper businessMapper;

    @Override
    public List<Map<String, Object>> selectBusiness(Map map){
        List<Map<String, Object>> businessList = businessMapper.selectBusiness(map);

        return businessList;
    }

    @Override
    public int selectBusinessCount(Map map){
        int businessCount = businessMapper.selectBusinessCount(map);
        return businessCount;
    }

    @Override
    public Map<String, Object> selectBusinessById(Long id){
        Map<String, Object> businessMap = businessMapper.selectBusinessById(id);
        return businessMap;
    }


    @Override
    public Map<String, Object> selectBusinessByBusinessId(String id){
        Map<String, Object> businessMap = businessMapper.selectBusinessByBusinessId(id);
        return businessMap;
    }


    @Override
    public void insertBusiness(Map map){
        businessMapper.insertBusiness(map);
    }

    @Override
    public void updateBusiness(Map map){
        businessMapper.updateBusiness(map);
    }

    @Override
    public boolean deleteById(long id){
        int count = businessMapper.deleteById(id);
        return count > 0 ? true : false;
    }


}
