package com.egao.common.system.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.AdConstant;
import com.egao.common.core.UploadConstant;
import com.egao.common.core.utils.AnalyticsUtil;
import com.egao.common.core.utils.CostUtil;
import com.egao.common.core.utils.DateUtil;
import com.egao.common.core.utils.HttpUtil;
import com.egao.common.system.mapper.ItemsMapper;
import com.egao.common.system.mapper.OverallMapper;
import com.egao.common.system.service.CertificateService;
import com.egao.common.system.service.CostService;
import com.egao.common.system.service.ItemsService;
import com.google.api.services.analytics.model.AccountSummary;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.Param;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

/**
 * <p>
 * 菜单表 服务实现类
 * </p>
 *
 * @author hs
 * @since 2020-10-10
 */
@Service
public class ItemsServiceImpl implements ItemsService {

    private Logger logger = LoggerFactory.getLogger("ItemsServiceImpl");

    @Autowired
    public ItemsMapper itemsMapper;

    @Autowired
    private CertificateService certificateService;

    @Autowired
    public OverallMapper overallMapper;

    @Autowired
    private CostService costService;


    public List<Map<String, Object>> selectItems(Map map){
        List<Map<String, Object>> itemsList = itemsMapper.selectItems(map);
        return itemsList;
    }

    public int selectItemsCount(Map map){
        int itemsCount = itemsMapper.selectItemsCount(map);
        return itemsCount;
    }

    public List<Map<String, Object>> selectItemsByJobNumber(Map map){
        List<Map<String, Object>> itemsList = itemsMapper.selectItemsByJobNumber(map);
        return itemsList;
    }

    @Override
    public Map<String, Object> selectItemsById(String id){
        Map<String, Object> itemMap = itemsMapper.selectItemsById(id);
        return itemMap;
    }

    @Override
    public Map<String, Object> selectItemsByName(String name){
        Map<String, Object> itemMap = itemsMapper.selectItemsByName(name);
        return itemMap;
    }


    @Override
    public void insertItems(Map map){
        itemsMapper.insertItems(map);

    }

    @Override
    public void updateItems(Map map){
        itemsMapper.updateItems(map);
    }

    @Override
    public void deleteAllItems(){
        itemsMapper.deleteAllItems();
    }




    public Map<String, Object> selectJobNumberById(@Param("id") String id){
        Map<String, Object> jobNumberMap = itemsMapper.selectJobNumberById(id);
        return jobNumberMap;
    }

    public List<Map<String, Object>> selectJobNumberByItemsId(@Param("itemsId") String itemsId){
        List<Map<String, Object>> jobNumberList = itemsMapper.selectJobNumberByItemsId(itemsId);
        return jobNumberList;
    }

    public List<Map<String, Object>> selectItemsCanBind(@Param("itemsId") Integer type){
        List<Map<String, Object>> itemsList = itemsMapper.selectItemsCanBind(type);
        for(Map<String, Object> itemMap : itemsList){
            Long value = (Long)itemMap.get("value");
            Long name = (Long)itemMap.get("name");
            itemMap.put("value", String.valueOf(value));
            itemMap.put("name", String.valueOf(name));
        }
        return itemsList;
    }

    public void insertJobNumber(Map map){
        itemsMapper.insertItems(map);
    }

    @Override
    public void deleteByType(Integer type){
        itemsMapper.deleteByType(type);
    }

    public void deleteByBusinessId(Long business, Integer type){
        itemsMapper.deleteByBusinessId(business, type);
    }


    public void syncGoogleItemsData(){
        // 同步GA站点
        Map maps = new HashMap();
        List<Map<String, Object>> certificateList = certificateService.selectAllCertificate(maps);

        if(certificateList.size() > 0){
            Map<String, Object> certificateMap = certificateList.get(0);
            String serviceAccountId = (String)certificateMap.get("service_account_id");
            String path = (String)certificateMap.get("path");


            File orgFile = new File(File.listRoots()[UploadConstant.UPLOAD_DIS_INDEX], UploadConstant.UPLOAD_DIR + path);

            List<AccountSummary> itemList = AnalyticsUtil.getItems(serviceAccountId, orgFile.getPath());

            System.out.println("itemList:"+itemList);

            if(itemList != null && itemList.size() > 0){
                //            itemsService.deleteAllItems();
                itemsMapper.deleteByType(0);
                for(AccountSummary item : itemList){
                    String id = item.getId();
                    String name = item.getName();

                    Map map = new HashMap();
                    map.put("id", id);
                    map.put("name", name);
                    map.put("type", 0);

                    System.out.println("map："+map);
                    logger.info("map："+map);
                    itemsMapper.insertItems(map);
                }
            }
        }


    }

    public void syncFacebookItemsData(){

        // 同步FB站点
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("access_token", AdConstant.ACCESS_TOKEN);
        params.add("fields", "id,name,account_id");

        String url = AdConstant.GRAPH_URL + AdConstant.BUSINESS_ID + "/client_ad_accounts?";

        System.out.println("url:"+url);

        String fields = "id,name,account_id";
        url = url + "access_token=" + AdConstant.ACCESS_TOKEN + "&fields=" + fields;
        String result = HttpUtil.get(url, params);
//            String result = "{\"data\":[{\"account_id\":\"2523536311212491\",\"id\":\"act_2523536311212491\",\"name\":\"FAYN-MS-dafunia02\"},{\"account_id\":\"578026722972031\",\"id\":\"act_578026722972031\",\"name\":\"FAYN-MS-dafunia01\"},{\"account_id\":\"837155403286860\",\"id\":\"act_837155403286860\",\"name\":\"FADM-MS-sonsoulier04\"},{\"account_id\":\"381293539350908\",\"id\":\"act_381293539350908\",\"name\":\"FADM-MS-sonsoulier03\"},{\"account_id\":\"2292495061017464\",\"id\":\"act_2292495061017464\",\"name\":\"FADM-MS-sonsoulier02\"},{\"account_id\":\"532421010599795\",\"id\":\"act_532421010599795\",\"name\":\"FADM-MS-sonsoulier01\"},{\"account_id\":\"2189317301321438\",\"id\":\"act_2189317301321438\",\"name\":\"FADM-MS-Shoeri03\"},{\"account_id\":\"292436271463301\",\"id\":\"act_292436271463301\",\"name\":\"FADM-MS-Shoeri04\"},{\"account_id\":\"231919237755305\",\"id\":\"act_231919237755305\",\"name\":\"FADM-MS-Shoeri05\"},{\"account_id\":\"2160394957354192\",\"id\":\"act_2160394957354192\",\"name\":\"FADM-MS-Shoeri01\"},{\"account_id\":\"1011028935762570\",\"id\":\"act_1011028935762570\",\"name\":\"FADM-MS-Shoeri02\"}],\"paging\":{\"cursors\":{\"after\":\"QVFIUnNkZA0xnbVBRT2Y2aUJqSjZAlOHVNTDc3Nk4tNEVVSzVzMFNLRFhtaTFFQnJ0b3QtNkV2ejl3cWcxejZARMm5USHlvWVByel9CUWlPQ2VRREwtQjI4V053\",\"before\":\"QVFIUlREOHNaYXpkSV9MRWRjYlREaUR1WnI0ek55VEhwT25CZAlNtczlmeXA1MWJ6MHVtbjBJeEtpN1psZAXVmMmthaVhVUV8zRENSRXB1aDdFUzkxMkZASM0tn\"}}}";

        System.out.println("result:"+result);

        logger.info("站点管理 result1:"+result);

        JSONObject jsonObject = JSONObject.parseObject(result);
        JSONArray dataArr = jsonObject.getJSONArray("data");

        Map map0 = new HashMap();
        map0.put("keyword", "");
        map0.put("page", 0);
        map0.put("rows", 100);
        List<Map<String, Object>> itemsList = itemsMapper.selectItems(map0);
        logger.info("itemsLists:" + itemsList.size());

        if(dataArr != null && dataArr.size() > 0){

            itemsMapper.deleteByType(1);
            for(int i = 0; i < dataArr.size(); i++){

                JSONObject jsonObj = dataArr.getJSONObject(i);
                Long id = jsonObj.getLong("account_id");
                String name = jsonObj.getString("name");

                Map map = new HashMap();
                map.put("id", id);
                map.put("name", name);
                map.put("type", 1);
                System.out.println("map:"+map);
                itemsMapper.insertItems(map);

            }
        }

    }

    public List<Map<String, Object>> selectItemsByType(Integer type){
        List<Map<String, Object>> itemsList = itemsMapper.selectItemsByType(type);
        return itemsList;
    }

    public List<Map<String, Object>> selectItemsByType2(Integer type){
        List<Map<String, Object>> itemsList = itemsMapper.selectItemsByType2(type);
        return itemsList;
    }

    public List<Map<String, Object>> selectAllItemsByType(Integer type){
        List<Map<String, Object>> itemsList = itemsMapper.selectAllItemsByType(type);
        return itemsList;
    }

    public List<Map<String, Object>> selectItemsByUserIdAndType(Integer userId, Integer type){
        List<Map<String, Object>> itemsList = itemsMapper.selectItemsByUserIdAndType(userId, type);
        return itemsList;
    }


    @Override
    public void updateBindingStatusById(Long id, Integer bindingStatus){
        itemsMapper.updateBindingStatusById(id, bindingStatus);
    }




    public List<Map<String, Object>> selectItem(Map map){
        List<Map<String, Object>> itemList = itemsMapper.selectItem(map);
        return itemList;
    }

    public int selectItemCount(Map map){
        int itemCount = itemsMapper.selectItemCount(map);
        return itemCount;
    }

    public List<Map<String, Object>> selectItemByParentId(Long parentId){
        List<Map<String, Object>> itemList = itemsMapper.selectItemByParentId(parentId);
        return itemList;
    }



    @Override
    public Long insertItem(Map map){
        int count = itemsMapper.insertItem(map);
        Long id = (Long)map.get("id");
        return id;
    }

    @Override
    public void updateItem(Map map){
        itemsMapper.updateItem(map);
    }

    @Override
    public void deleteItemById(Long id){
        itemsMapper.deleteItemById(id);
    }

    @Override
    public void deleteItemByParentId(Long parentId){
        itemsMapper.deleteItemByParentId(parentId);
    }

    @Override
    public Map<String, Object> selectItemById(Long id){
        Map<String, Object> itemMap = itemsMapper.selectItemById(id);
        return itemMap;
    }

    public List<Map<String, Object>> selectAdAccountByItemId(Long itemId, Integer accountType){
        List<Map<String, Object>> itemList = itemsMapper.selectAdAccountByItemId(itemId, accountType);
        return itemList;
    }

    public List<Map<String, Object>> selectFBAdAccountByItemId(Long itemId, Integer accountType){
        List<Map<String, Object>> itemList = itemsMapper.selectFBAdAccountByItemId(itemId, accountType);
        return itemList;
    }

    public List<Map<String, Object>> selectItemCost(Map map){
        List<Map<String, Object>> itemCostList = itemsMapper.selectItemCost(map);
        return itemCostList;
    }

    public static void dateGroupBy(Map map, List<Map<String, Object>> tempList, List<Map<String, Object>> itemList){

        Integer groupStatus = (Integer)map.get("groupStatus"); // 决定是否按照渠道进行分组

        for(Map<String, Object> itemMap : itemList){
            Date dates = (Date)itemMap.get("date");
            Long itemsId = (Long)itemMap.get("itemsId");
            String jobNumer = (String)itemMap.get("job_number");
            Integer channelId = (Integer)itemMap.getOrDefault("channelId", 0);


            boolean isGather = false;

            for(Map<String, Object> tempMap : tempList){
                Date tempDates = (Date)tempMap.get("date");
                Long tempItemsId = (Long)tempMap.get("itemsId");
                String tempJobNumer = (String)tempMap.get("job_number");
                Integer tempChannelId = (Integer)tempMap.getOrDefault("channelId", 0);


                jobNumer = StringUtils.isEmpty(jobNumer) ? "" : jobNumer;
                tempJobNumer = StringUtils.isEmpty(tempJobNumer) ? "" : tempJobNumer;

                // 如果有进行分组操作，则使用渠道ID和站点ID同时进行数据汇总
/*
                boolean isChannel = true;
                if(groupStatus == 1){
                    isChannel = channelId.equals(tempChannelId);
                }

                // id大于10000000000说明是FB广告账户，把它放到其它里面
                if(itemsId > 10000000000l && tempItemsId > 10000000000l && isChannel){

                    gatherMap(itemMap, tempMap);
//                    itemMap.put("itemsName", "Facebook");
                    isGather = true;
                }else
                if(itemsId.equals(tempItemsId) && isChannel){
                    System.out.println("---汇总开始 itemsId："+itemsId + " jobNumer："+jobNumer);

                    gatherMap(itemMap, tempMap);

                    isGather = true;
//                    break;

                }
*/


                if(dates.equals(tempDates)){
//                    System.out.println("---汇总开始 itemsId："+itemsId + " jobNumer："+jobNumer);
//                    System.out.println("dates："+dates);
//                    System.out.println("tempDates："+tempDates);
                    gatherMap(itemMap, tempMap);

                    isGather = true;
                }

            }

            // 如果没有汇总，就直接添加
            if(!isGather){
                tempList.add(itemMap);
            }

            // 大于10000000000的ID说明是Facebook
/*
            Long itemsIdOther = (Long)itemMap.get("itemsId");
            if(itemsIdOther > 10000000000l){
                itemMap.put("itemsName", "Facebook");
                System.out.println("这是其它");
            }
*/

        }

        System.out.println("555tempList1："+JSONArray.toJSONString(tempList));
        for(Map<String, Object> tempMap : tempList){
            BigDecimal revenue = (BigDecimal)tempMap.get("revenue");
            BigDecimal cost = (BigDecimal)tempMap.get("cost");

            tempMap.put("revenue", revenue.setScale(2, BigDecimal.ROUND_HALF_UP));
            tempMap.put("cost", cost.setScale(2, BigDecimal.ROUND_HALF_UP));
        }
        System.out.println("555tempList2："+JSONArray.toJSONString(tempList));
    }


    public static void itemGroupBy(Map map, List<Map<String, Object>> tempList, List<Map<String, Object>> itemList){

        Integer groupStatus = (Integer)map.get("groupStatus"); // 决定是否按照渠道进行分组

        for(Map<String, Object> itemMap : itemList){
            Long itemsId = (Long)itemMap.get("itemsId");
            String jobNumer = (String)itemMap.get("job_number");
            Integer channelId = (Integer)itemMap.getOrDefault("channelId", 0);


            boolean isGather = false;

            for(Map<String, Object> tempMap : tempList){
                Long tempItemsId = (Long)tempMap.get("itemsId");
                String tempJobNumer = (String)tempMap.get("job_number");
                Integer tempChannelId = (Integer)tempMap.getOrDefault("channelId", 0);


                jobNumer = StringUtils.isEmpty(jobNumer) ? "" : jobNumer;
                tempJobNumer = StringUtils.isEmpty(tempJobNumer) ? "" : tempJobNumer;

                // 如果有进行分组操作，则使用渠道ID和站点ID同时进行数据汇总
                boolean isChannel = true;
                if(groupStatus == 1){
                    isChannel = channelId.equals(tempChannelId);
                }

                // id大于10000000000说明是FB广告账户，把它放到其它里面
                if(itemsId > 10000000000l && tempItemsId > 10000000000l && isChannel){

                    gatherMap(itemMap, tempMap);
//                    itemMap.put("itemsName", "Facebook");
                    isGather = true;
                }else
                if(itemsId.equals(tempItemsId) && isChannel){
                    System.out.println("---汇总开始 itemsId："+itemsId + " jobNumer："+jobNumer);

                    gatherMap(itemMap, tempMap);

                    isGather = true;
//                    break;

                }
            }

            // 如果没有汇总，就直接添加
            if(!isGather){
                tempList.add(itemMap);
            }

            // 大于10000000000的ID说明是Facebook
            Long itemsIdOther = (Long)itemMap.get("itemsId");
            if(itemsIdOther > 10000000000l){
                itemMap.put("itemsName", "Facebook");
                System.out.println("这是其它");
            }

        }

        System.out.println("555tempList1："+JSONArray.toJSONString(tempList));
        for(Map<String, Object> tempMap : tempList){
            BigDecimal revenue = (BigDecimal)tempMap.get("revenue");
            BigDecimal cost = (BigDecimal)tempMap.get("cost");

            tempMap.put("revenue", revenue.setScale(2, BigDecimal.ROUND_HALF_UP));
            tempMap.put("cost", cost.setScale(2, BigDecimal.ROUND_HALF_UP));
        }
        System.out.println("555tempList2："+JSONArray.toJSONString(tempList));
    }




    public static void gatherMap(Map<String, Object> itemMap, Map<String, Object> tempMap){
        System.out.println("gatherMap tempMap："+tempMap);

        BigDecimal zero = new BigDecimal(0.00);

        BigDecimal tempRevenue = (BigDecimal)tempMap.getOrDefault("revenue", zero);
        BigDecimal tempCost = (BigDecimal)tempMap.getOrDefault("cost", zero);
//                    BigDecimal tempLogisticCost = BigDecimal.valueOf((Float)tempMap.get("logisticCost"));
//                    BigDecimal tempGoodsCost = BigDecimal.valueOf((Float)tempMap.get("goodsCost"));
//                    BigDecimal tempRefund = BigDecimal.valueOf((Float)tempMap.get("refund"));
//                    BigDecimal tempToolCost = BigDecimal.valueOf((Float)tempMap.get("toolCost"));
//                    BigDecimal tempPassCost = BigDecimal.valueOf((Float)tempMap.get("passCost"));
//                    BigDecimal tempOperateCost = BigDecimal.valueOf((Float)tempMap.get("operateCost"));


        BigDecimal tempLogisticCost = (BigDecimal)tempMap.getOrDefault("logisticCost", zero);
        BigDecimal tempGoodsCost = (BigDecimal)tempMap.getOrDefault("goodsCost", zero);
        BigDecimal tempRefund = (BigDecimal)tempMap.getOrDefault("refund", zero);
        BigDecimal tempToolCost = (BigDecimal)tempMap.getOrDefault("toolCost", zero);
        BigDecimal tempPassCost = (BigDecimal)tempMap.getOrDefault("passCost", zero);
        BigDecimal tempOperateCost = (BigDecimal)tempMap.getOrDefault("operateCost", zero);

        System.out.println("gatherMap itemMap："+itemMap);

        BigDecimal revenue = (BigDecimal)itemMap.getOrDefault("revenue", zero);
        BigDecimal cost = (BigDecimal)itemMap.getOrDefault("cost", zero);



        BigDecimal logisticCost = (BigDecimal)itemMap.getOrDefault("logisticCost", zero);
        BigDecimal goodsCost =  (BigDecimal)itemMap.getOrDefault("goodsCost", zero);
        BigDecimal refund =  (BigDecimal)itemMap.getOrDefault("refund", zero);
        BigDecimal toolCost =  (BigDecimal)itemMap.getOrDefault("toolCost", zero);
        BigDecimal passCost =  (BigDecimal)itemMap.getOrDefault("passCost", zero);
        BigDecimal operateCost =  (BigDecimal)itemMap.getOrDefault("operateCost", zero);


        System.out.println("tempLogisticCost："+tempLogisticCost);
        System.out.println("logisticCost："+logisticCost);


        revenue = tempRevenue.add(revenue);
        tempMap.put("revenue", revenue.setScale(2, BigDecimal.ROUND_HALF_UP));
        cost = tempCost.add(cost);
        tempMap.put("cost", cost.setScale(2, BigDecimal.ROUND_HALF_UP));
        logisticCost = tempLogisticCost.add(logisticCost);
        tempMap.put("logisticCost", logisticCost.setScale(2, BigDecimal.ROUND_HALF_UP));
        goodsCost = tempGoodsCost.add(goodsCost);
        tempMap.put("goodsCost", goodsCost.setScale(2, BigDecimal.ROUND_HALF_UP));
        refund = tempRefund.add(refund);
        tempMap.put("refund", refund.setScale(2, BigDecimal.ROUND_HALF_UP));
        toolCost = tempToolCost.add(toolCost);
        tempMap.put("toolCost", toolCost.setScale(2, BigDecimal.ROUND_HALF_UP));
        passCost = tempPassCost.add(passCost);
        tempMap.put("passCost", passCost.setScale(2, BigDecimal.ROUND_HALF_UP));
        operateCost = tempOperateCost.add(operateCost);
        tempMap.put("operateCost", operateCost.setScale(2, BigDecimal.ROUND_HALF_UP));

        BigDecimal change = new BigDecimal(100);

        boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;

        tempMap.put("logisticCostRatio", revenueZero ? zero : logisticCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(0, BigDecimal.ROUND_HALF_UP));
        tempMap.put("goodsCostRatio", revenueZero ? zero : goodsCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(0, BigDecimal.ROUND_HALF_UP));
        tempMap.put("refundRate", revenueZero ? zero : refund.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(0, BigDecimal.ROUND_HALF_UP));
        tempMap.put("toolCostRatio", revenueZero ? zero : toolCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(0, BigDecimal.ROUND_HALF_UP));
        tempMap.put("passCostRatio", revenueZero ? zero : passCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(0, BigDecimal.ROUND_HALF_UP));
        tempMap.put("operateCostRatio", revenueZero ? zero : operateCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(0, BigDecimal.ROUND_HALF_UP));



        // ROAS
        boolean isZero = cost.compareTo(BigDecimal.ZERO)==0;
        BigDecimal roas = new BigDecimal(0.00);

        tempMap.put("roas", isZero ? roas : revenue.divide(cost,2, RoundingMode.HALF_UP));

        // 广告成本占比
        boolean zero2 = revenue.compareTo(BigDecimal.ZERO)==0;
        BigDecimal costProportion = new BigDecimal(0.00);

        tempMap.put("costProportion", zero2 ? costProportion : (cost.divide(revenue,6, RoundingMode.HALF_UP)).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));


        // 利润
        // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
        BigDecimal profit = revenue.subtract(cost).subtract(logisticCost).subtract(goodsCost).subtract(operateCost).subtract(refund).subtract(toolCost).subtract(passCost);

        // 利润率
        // 利润/收入
//                    boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;
        BigDecimal profitRate = new BigDecimal(0.00);

        profitRate = revenueZero ? profitRate : profit.divide(revenue, 2, BigDecimal.ROUND_HALF_UP);

        tempMap.put("profitRate", roundHalfUp(profitRate.multiply(change).setScale(0, BigDecimal.ROUND_HALF_UP)));
        // 利润
        tempMap.put("profit", roundHalfUp(profit.setScale(2, BigDecimal.ROUND_HALF_UP)));

    }

    public static BigDecimal roundHalfUp(BigDecimal value){
        return value.setScale(2, BigDecimal.ROUND_HALF_UP);
    }


    public List<Map<String, Object>> selectItemData(Map map){

        String startTime = (String)map.get("startTime");
        String endTime = (String)map.get("endTime");


        System.out.println("--->selectItemData start："+map);
        System.out.println("startTime："+startTime);
        System.out.println("endTime："+endTime);
        long startTimeLong = DateUtil.parseDate(startTime, "yyyy-MM-dd").getTime();
        long endTimeLong = DateUtil.parseDate(endTime, "yyyy-MM-dd").getTime();
        String startMonth = DateUtil.timestampToTime(startTimeLong, "yyyy-MM");
        String endMonth = DateUtil.timestampToTime(endTimeLong, "yyyy-MM");


        boolean monthIfEquals = DateUtil.monthIfEquals(startTime, endTime); // 比如两个日期月份是否相等
        System.out.println("monthIfEquels：" + monthIfEquals);
        List<Map<String, Object>> itemList = itemsMapper.selectItemData(map);
        System.out.println("selectItemData itemList："+JSONArray.toJSONString(itemList));


        Map map2 = new HashMap();
//        map2.put("startTime", startMonth);
//        map2.put("endTime", endMonth);
        map2.put("page", 0);
        map2.put("rows", 1000);
        List<Map<String, Object>> costList = costService.selectCost(map2);
        List<Map<String, Object>> overallList = overallMapper.selectOverall(map2);

        String aMonthFirstDayTime = DateUtil.getAMonthFirstDay(startTimeLong); // 获取一个月第一天
        String aMonthLastDayTime = DateUtil.getAMonthLastDay(endTimeLong); // 获取一个月最后一天
        Map map3 = new HashMap();
        map3.put("itemsId", map.get("itemsId"));
        map3.put("startTime", aMonthFirstDayTime);
        map3.put("endTime", aMonthLastDayTime);
        map3.put("jobNumer", map.get("jobNumer"));
        map3.put("adAccount", map.get("adAccount"));
        map3.put("channelId", map.get("channelId"));

        System.out.println("map388："+map3);
        List<Map<String, Object>> itemCostList = itemsMapper.selectItemCost(map3);

        System.out.println("itemCostList json："+JSONArray.toJSONString(itemCostList));

        List<Map<String, Object>> tempList = new ArrayList<>();
        CostUtil.censusItemCost(map, tempList, itemList, costList, overallList, itemCostList, startTime, endTime);
        System.out.println("pre itemList："+JSONArray.toJSONString(itemList));
        itemGroupBy(map, tempList, itemList);
        System.out.println("after tempList："+JSONArray.toJSONString(tempList));

        return tempList;

    }




    public List<Map<String, Object>> selectChartData(Map map){


        String startTime = (String)map.get("startTime");
        String endTime = (String)map.get("endTime");


        System.out.println("--->selectItemData start："+map);
        System.out.println("startTime："+startTime);
        System.out.println("endTime："+endTime);
        long startTimeLong = DateUtil.parseDate(startTime, "yyyy-MM-dd").getTime();
        long endTimeLong = DateUtil.parseDate(endTime, "yyyy-MM-dd").getTime();
        String startMonth = DateUtil.timestampToTime(startTimeLong, "yyyy-MM");
        String endMonth = DateUtil.timestampToTime(endTimeLong, "yyyy-MM");


        boolean monthIfEquals = DateUtil.monthIfEquals(startTime, endTime); // 比如两个日期月份是否相等
        System.out.println("monthIfEquels：" + monthIfEquals);
//        List<Map<String, Object>> itemList = itemsMapper.selectItemData(map);
        List<Map<String, Object>> itemList = itemsMapper.selectChartData(map);

        System.out.println("selectChartData itemList2："+JSONArray.toJSONString(itemList));

        Map map2 = new HashMap();
        map2.put("page", 0);
        map2.put("rows", 1000);
        List<Map<String, Object>> costList = costService.selectCost(map2);
        List<Map<String, Object>> overallList = overallMapper.selectOverall(map2);

        String aMonthFirstDayTime = DateUtil.getAMonthFirstDay(startTimeLong); // 获取一个月第一天
        String aMonthLastDayTime = DateUtil.getAMonthLastDay(endTimeLong); // 获取一个月最后一天
        Map map3 = new HashMap();
        map3.put("itemsId", map.get("itemsId"));
        map3.put("startTime", aMonthFirstDayTime);
        map3.put("endTime", aMonthLastDayTime);
        map3.put("jobNumer", map.get("jobNumer"));
        map3.put("adAccount", map.get("adAccount"));
        map3.put("channelId", map.get("channelId"));

        System.out.println("map388："+map3);
        List<Map<String, Object>> itemCostList = itemsMapper.selectItemCost(map3);

        System.out.println("itemCostList json2："+JSONArray.toJSONString(itemCostList));

        List<Map<String, Object>> tempList = new ArrayList<>();
        CostUtil.censusItemCost(map, tempList, itemList, costList, overallList, itemCostList, startTime, endTime);
        System.out.println("pre itemList2："+JSONArray.toJSONString(itemList));
        dateGroupBy(map, tempList, itemList);
        System.out.println("after tempList2："+JSONArray.toJSONString(tempList));

        return tempList;

    }

}
