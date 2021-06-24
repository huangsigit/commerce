package com.egao.common.system.service.impl;

import com.egao.common.system.mapper.UserItemMapper;
import com.egao.common.system.mapper.ChannelMapper;
import com.egao.common.system.mapper.ItemsMapper;
import com.egao.common.system.service.UserItemService;
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
public class UserItemServiceImpl implements UserItemService {

    @Autowired
    public UserItemMapper userItem;

    public List<Map<String, Object>> selectUserItem(Map map){
        List<Map<String, Object>> costList = userItem.selectUserItem(map);

        return costList;
    }

    public int selectUserItemCount(Map map){
        int costCount = userItem.selectUserItemCount(map);
        return costCount;
    }

    public List<Map<String, Object>> selectUserItemByUserId(Integer userId){
        List<Map<String, Object>> costList = userItem.selectUserItemByUserId(userId);
        return costList;
    }


    @Override
    public boolean insertUserItem(Map map){

        int count = userItem.insertUserItem(map);
        if(count > 0){
            System.out.println("渠道成本插入成本");
            return true;
        }

        return false;
    }


    @Override
    public void updateUserItem(Map map){
        userItem.updateUserItem(map);
    }


    @Override
    public boolean deleteUserItemById(Integer id){
        int count = userItem.deleteUserItemById(id);
        return count > 0 ? true : false;
    }

    @Override
    public boolean deleteUserItemByUserId(Integer userId){
        int count = userItem.deleteUserItemByUserId(userId);
        return count > 0 ? true : false;
    }


}
