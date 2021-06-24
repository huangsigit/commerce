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
public interface AdMapper {



    public List<Map<String, Object>> selectAd(Map map);

    public int selectAdCount(Map map);

    public Map<String, Object> selectAllSum(Map map);

    public List<Map<String, Object>> selectAdByItemsId(@Param("itemsId") long itemsId);

    public void insertAd(Map map);

    public List<Map<String, Object>> selectAdAccountByItemsIdAndJobNumber(@Param("itemsId") long itemsId, @Param("jobNumber") String jobNumber);

    public List<Map<String, Object>> selectAdItems(Map map);

    void insertAds(Map map);

    int updateAds(Map map);

    List<Map<String, Object>> selectAdsByItemsId(@Param("itemsId") long itemsId);

    List<Map<String, Object>> selectAdGroupByJobNumber(@Param("itemsId") long itemsId, @Param("jobNumber") String jobNumber);

    List<Map<String, Object>> selectAdGroupByAdAccount(@Param("itemsId") long itemsId, @Param("jobNumber") String jobNumber);

    List<Map<String, Object>> selectAdAccountGroupByItemsId(@Param("itemsId") long itemsId);


    public List<Map<String, Object>> selectAdChannel(Map map);

//    public List<Map<String, Object>> selectChannelData(Map map);

//    public int selectChannelDataCount(Map map);


    public int selectAdChannelCount(Map map);


    void deleteByType(Map map);

    void deleteByItemsId(Map map);

    void deleteByAdAccount(Map map);

    Float selectAMonthRevenue(Map map);

}
