package com.egao.common.system.service;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * 广告表 服务类
 * </p>
 *
 * @author hs
 * @since 2019-10-10
 */
public interface AdService {

    public List<Map<String, Object>> selectAd(Map map);

    public int selectAdCount(Map map);

    public List<Map<String, Object>> selectAdByItemsId(long itemsId);

    void insertAd(Map map);


    public List<Map<String, Object>> selectAdAccountByItemsIdAndJobNumber(long itemsId, String jobNumber);

    public List<Map<String, Object>> selectAdItems(Map map);

    public Map<String, Object> selectAllSum(List<Map<String, Object>> adList, Map map);

    void insertAds(Map map);

    int updateAds(Map map);

    List<Map<String, Object>> selectAdsByItemsId(long itemsId);

    List<Map<String, Object>> selectAdGroupByJobNumber(long itemsId, String jobNumber);

    List<Map<String, Object>> selectAdGroupByAdAccount(long itemsId, String jobNumber);

    List<Map<String, Object>> selectAdAccountGroupByItemsId(long itemsId);


    public List<Map<String, Object>> selectAdChannel(Map map);

//    public List<Map<String, Object>> selectChannelData(Map map);

//    public int selectChannelDataCount(Map map);


    public int selectAdChannelCount(Map map);

    void deleteByType(Map map);

    void deleteByItemsId(Map map);

    void deleteByAdAccount(Map map);

    void syncGoogleData();

    void syncFacebookData();

    Float selectAMonthRevenue(Map map);

}
