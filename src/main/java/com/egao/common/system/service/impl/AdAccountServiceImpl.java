package com.egao.common.system.service.impl;

import com.egao.common.system.mapper.AdAccountMapper;
import com.egao.common.system.service.AdAccountService;
import com.egao.common.system.service.AdAccountService;
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
public class AdAccountServiceImpl implements AdAccountService {

    private Logger logger = LoggerFactory.getLogger("AdAccountServiceImpl");

    @Autowired
    public AdAccountMapper adAccountMapper;

    public List<Map<String, Object>> selectAdAccountByType(Integer type){
        List<Map<String, Object>> adAccountList = adAccountMapper.selectAdAccountByType(type);
        return adAccountList;
    }

    public List<Map<String, Object>> selectAdAccountByItemsId(Long itemsId){
        List<Map<String, Object>> adAccountList = adAccountMapper.selectAdAccountByItemsId(itemsId);
        return adAccountList;
    }

    public Map<String, Object> selectAdAccountById(Long id){
        Map<String, Object> adAccountMap = adAccountMapper.selectAdAccountById(id);
        return adAccountMap;
    }

    public void insertAdAccount(Map map){
        adAccountMapper.insertAdAccount(map);
    }

    public void deleteById(long id){
        adAccountMapper.deleteById(id);
    }


}
