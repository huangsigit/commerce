package com.egao.common.system.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.Cache;
import com.egao.common.core.UploadConstant;
import com.egao.common.core.annotation.OperLog;
import com.egao.common.core.utils.AnalyticsUtil;
import com.egao.common.core.utils.DateUtil;
import com.egao.common.core.utils.HttpUtil;
import com.egao.common.core.web.BaseController;
import com.egao.common.core.web.JsonResult;
import com.egao.common.system.entity.Role;
import com.egao.common.system.entity.User;
import com.egao.common.system.service.AdAccountService;
import com.egao.common.system.service.AdService;
import com.egao.common.system.service.CertificateService;
import com.egao.common.system.service.ItemsService;
import com.google.api.services.analytics.model.AccountSummary;
import com.google.api.services.analytics.model.ProfileSummary;
import com.google.api.services.analytics.model.WebPropertySummary;
import org.apache.commons.lang3.StringUtils;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

@Controller
@RequestMapping("/sys/items")
public class ItemsController extends BaseController {

    private Logger logger = LoggerFactory.getLogger("ItemsController");

//    @Autowired
//    private CertificateService certificateService;

    @Autowired
    private ItemsService itemsService;

    @Autowired
    private CertificateService certificateService;

    @Autowired
    private AdService adService;

    @Autowired
    private AdAccountService adAccountService;


    public static String GRAPH_URL = "https://graph.facebook.com/v7.0/";

    public static String ACCESS_TOKEN = "EAAH92JtasVMBAJ2iHbMXEdLwzMZAH2PidkMGwvQbhFZCZAAcPmUHOxfwaPfNg4M3vXCBonOVZAHLIrj7gdZCJqT9pQs8CAMGrBp7ECuNKOdFIO5txnP3UylNAI959oXBqp1hZAJloEBqSvVdt3hVhXYDu7WGdoZCgZCqrqX0PVE5LKKdGtlzQMxZBmrY8YWjQARUZD";

    public static String BUSINESS_ID = "144436283227029";

    @RequiresPermissions("sys:items:view")
    @RequestMapping()
    public String view(Model model) {

        List<Map<String, Object>> gaItemsList = itemsService.selectItemsByType(0);


        System.out.println("view gaItemsList："+gaItemsList);
//        List<Map<String, Object>> fbList = itemsService.selectItemsByType(1);

//        List<Map<String, Object>> gaAdAccountList = adAccountService.selectAdAccountByType(0);
        List<Map<String, Object>> gaAdAccountList = new ArrayList<>();

//        List<Map<String, Object>> fbAdAccountList = adAccountService.selectAdAccountByType(1);
        List<Map<String, Object>> fbAdAccountList = itemsService.selectItemsCanBind(1);


        model.addAttribute("gaItemsList", JSON.toJSONString(gaItemsList));
        model.addAttribute("gaAdAccountList", JSON.toJSONString(gaAdAccountList));
        model.addAttribute("fbAdAccountList", JSON.toJSONString(fbAdAccountList));

        model.addAttribute("itemList", gaItemsList);


        System.out.println("view gaAdAccountList："+gaAdAccountList);
        System.out.println("view fbAdAccountList："+JSON.toJSONString(fbAdAccountList));

        return "system/items.html";
    }



    @OperLog(value = "站点管理", desc = "分页查询")
    @RequiresPermissions("sys:items:list")
    @ResponseBody
    @RequestMapping(value = "/page", produces = "application/json;charset=UTF-8")
    public JsonResult list(HttpServletRequest request, @RequestParam(name = "keyword", required = false)String keyword
            , @RequestParam(name = "page", required = false)Integer page, @RequestParam(name = "limit", required = false)Integer limit) {



        System.out.println("站点管理 itemsTasks：" + DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH;mm:ss:SSS"));
        logger.info("站点管理 start:" + DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH;mm:ss:SSS"));



        System.out.println("page："+page);
        System.out.println("limit："+limit);

        Map map = new HashMap();

//        map.put("page", (page-1)*limit);
//        map.put("rows", limit);
        map.put("page", 0);
//        map.put("rows", 100);
        map.put("rows", 1000);
//        map.put("keyword", keyword);

//        List<Map<String, Object>> itemsList = itemsService.selectItems(map);
        List<Map<String, Object>> itemsList = itemsService.selectItem(map);


        int itemCount = 0;
        if(itemsList.size() > 0){
//            itemsList.get(0).put("hide", 0);
//            itemsList.get(1).put("hide", 0);

            for(Map<String, Object> itemMap : itemsList){
                String adAccountName = (String)itemMap.get("adAccountName");
                String adAccount = (String)itemMap.get("ad_account");
                Integer parentId = (Integer)itemMap.get("parent_id");
                String itemsName = (String)itemMap.get("itemsName");
                if(parentId != 0){
//                    itemMap.put("itemsName", "");
//                    itemMap.put("adAccountName", adAccountName == null ? adAccount : adAccountName);
                    itemMap.put("adAccountName", adAccount);
                }else{
                    itemMap.put("item_name", itemsName);
                }
            }

            itemCount = itemsService.selectItemCount(map);
        }

        List<Map<String, Object>> gaItemsList = itemsService.selectItemsByType(0);
        List<Map<String, Object>> gaAdAccountList = adAccountService.selectAdAccountByType(0);
//        List<Map<String, Object>> fbAdAccountList = adAccountService.selectAdAccountByType(1);
        List<Map<String, Object>> fbAdAccountList = itemsService.selectItemsCanBind(1);


        JsonResult data = JsonResult.ok(0, itemCount).put("data", itemsList).put("gaItemsList", gaItemsList)
                .put("gaAdAccountList", gaAdAccountList).put("fbAdAccountList", fbAdAccountList);
        System.out.println("站点管理 list data："+JSONObject.toJSON(data));

        return data;
    }



    @OperLog(value = "站点管理", desc = "添加数据")
    @RequiresPermissions("sys:items:add")
    @ResponseBody
    @RequestMapping(value = "/add")
    public JsonResult add(String json, @RequestParam(name = "itemsId", required = false)String itemsId
            , @RequestParam(name = "gaAccountSel", required = false)String gaAccountSel, @RequestParam(name = "fbAccountSel", required = false)String fbAccountSel) {

        try {

            System.out.println("站点管理 添加数据："+itemsId);
            System.out.println("站点管理 添加数据："+gaAccountSel);
            System.out.println("站点管理 添加数据："+fbAccountSel);

            Map map = new HashMap();

            map.put("parent_id", 0);
            map.put("item_id", itemsId);
            map.put("item_type", 0);
            map.put("account_type", 0);

            System.out.println("map:"+map);

/*
            if(true){
                return JsonResult.error("添加失败");
            }
*/


            Long id = itemsService.insertItem(map);

            String[] gaArr = gaAccountSel.split(",");//注意分隔符是需要转译
            System.out.println("gaArr："+gaArr[0]);
            System.out.println("gaArr length："+gaArr.length);
            if(gaArr.length > 0){
                for (int i = 0; i < gaArr.length; i++) {

                    // 没有谷歌广告账户
                    if(StringUtils.isEmpty(gaArr[i])){
                        continue;
                    }
                    map.put("parent_id", id);
                    map.put("ad_account", gaArr[i]);
                    map.put("item_type", 1);
                    map.put("account_type", 0);
                    map.put("item_name", "");
//                map.put("item_id", null);

                    System.out.println("ga map："+map);
                    itemsService.insertItem(map);

//                itemsService.updateBindingStatusById(Long.valueOf(gaArr[i]), 1); // 修改绑定状态

                }

                System.out.println("gaArr："+gaArr);

            }


            String[] fbArr = fbAccountSel.split(",");//注意分隔符是需要转译
            if(fbArr.length > 0){
                for (int i = 0; i < fbArr.length; i++) {

                    if(StringUtils.isEmpty(fbArr[i])){
                        continue;
                    }
                    map.put("parent_id", id);
                    map.put("ad_account", fbArr[i]);
                    map.put("item_type", 1);
                    map.put("account_type", 1);
                    map.put("item_name", "");
//                map.put("item_id", null);
                    System.out.println("fb map："+map);
                    itemsService.insertItem(map);

//                itemsService.updateBindingStatusById(Long.valueOf(fbArr[i]), 1); // 修改绑定状态

                }
                System.out.println("map:"+map);
            }


            List<Map<String, Object>> gaItemsList = itemsService.selectItemsByType(0);
            List<Map<String, Object>> gaAdAccountList = adAccountService.selectAdAccountByType(0);
//            List<Map<String, Object>> fbAdAccountList = adAccountService.selectAdAccountByType(1);
            List<Map<String, Object>> fbAdAccountList = itemsService.selectItemsCanBind(1);

            JsonResult data = JsonResult.ok("添加成功").put("gaItemsList", JSON.toJSONString(gaItemsList))
                    .put("gaAdAccountList", JSON.toJSONString(gaAdAccountList)).put("fbAdAccountList", JSON.toJSONString(fbAdAccountList));

            System.out.println("站点管理 data："+JSONObject.toJSON(data));

            return data;
        } catch (Exception e) {
            e.printStackTrace();
            return JsonResult.error("添加失败");
        }
    }



    @OperLog(value = "站点管理", desc = "修改数据")
    @RequiresPermissions("sys:items:update")
    @ResponseBody
    @RequestMapping(value = "/update")
    public JsonResult update(String json, @RequestParam(name = "id", required = false)Long id, @RequestParam(name = "itemsId", required = false)Long itemsId
            , @RequestParam(name = "gaAccountSel", required = false)String gaAccountSel, @RequestParam(name = "fbAccountSel", required = false)String fbAccountSel) {

        try {

            System.out.println("站点管理 修改数据："+itemsId);
            System.out.println("站点管理 修改数据："+gaAccountSel);
            System.out.println("站点管理 修改数据："+fbAccountSel);




/*
            if(true){
                return JsonResult.error("添加失败");
            }
*/

            Map map = new HashMap();


            Map<String, Object> itemMap = itemsService.selectItemById(itemsId);

            List<Map<String, Object>> itemList = itemsService.selectItemByParentId(id);
            System.out.println("---itemList itemList："+itemList);
            if(itemMap == null){

                // 判断是否是子
                Map<String, Object> itemMap2 = itemsService.selectItemById(id);
                if(itemMap2 != null){
                    Integer parentIdInt = (Integer)itemMap2.get("parent_id");
                    Long parentId = Long.valueOf(parentIdInt);


                    // 如果是父
                    if(parentId == 0){
//                    itemsService.deleteItemByParentId(id);
//                        channelService.deleteByParentId(id);
                        System.out.println("是父："+id);

                        itemsService.deleteItemByParentId(id);

                    }else{ // 如果是子
//                        channelService.deleteByParentId(parentId);
                        System.out.println("是子："+parentId);
                        id = parentId;

                        itemsService.deleteItemByParentId(parentId);


                    }
                }


                // 如果站点发生改变，直接删除站点并删除该站点下的广告账户，然后重新插入
                // 先删除父再删除子
//                itemsService.deleteItemByParentId(id);
//                itemsService.deleteItemById(id);



                map.put("id", id);
                map.put("parent_id", 0);
                map.put("item_id", itemsId);
                map.put("item_type", 0);
                map.put("account_type", 0);

                System.out.println("map:"+map);

//                id = itemsService.insertItem(map);

                itemsService.updateItem(map);



                // 处理GA
                String[] gaArr = gaAccountSel.split(",");//注意分隔符是需要转译
                for (int i = 0; i < gaArr.length; i++) {

                    map.put("parent_id", id);
                    map.put("ad_account", gaArr[i]);
                    map.put("item_type", 1);
                    map.put("account_type", 0);
                    map.put("item_name", "");
//                map.put("item_id", null);

                    System.out.println("ga map："+map);
                    itemsService.insertItem(map);

                }
                System.out.println("gaArr："+gaArr);

            }


            String[] fbArr = fbAccountSel.split(",");//注意分隔符是需要转译
            for (int i = 0; i < fbArr.length; i++) {

                map.put("parent_id", id);
                map.put("ad_account", fbArr[i]);
                map.put("item_type", 1);
                map.put("account_type", 1);
                map.put("item_name", "");
//                map.put("item_id", null);
                System.out.println("fb map："+map);

                // 判断FB广告账户是否存在
                for(Map<String, Object> itemMap2 : itemList){
                    String adAccount = (String)itemMap2.get("adAccount");
                    if(adAccount.equals(fbArr[i])){
                        System.out.println("已经存在 不需要插入");
                        continue;
                    }
                }

                itemsService.insertItem(map);

            }


            System.out.println("map:"+map);

            List<Map<String, Object>> gaItemsList = itemsService.selectItemsByType(0);
            List<Map<String, Object>> gaAdAccountList = adAccountService.selectAdAccountByType(0);
//            List<Map<String, Object>> fbAdAccountList = adAccountService.selectAdAccountByType(1);
            List<Map<String, Object>> fbAdAccountList = itemsService.selectItemsCanBind(1);

            JsonResult data = JsonResult.ok("添加成功").put("gaItemsList", JSON.toJSONString(gaItemsList))
                    .put("gaAdAccountList", JSON.toJSONString(gaAdAccountList)).put("fbAdAccountList", JSON.toJSONString(fbAdAccountList));

            System.out.println("站点管理 data："+JSONObject.toJSON(data));

            return data;
        } catch (Exception e) {
            e.printStackTrace();
            return JsonResult.error("添加失败");
        }
    }



    /**
     * 删除菜单
     */
    @OperLog(value = "站点管理", desc = "删除数据", result = true)
    @RequiresPermissions("sys:items:delete")
    @ResponseBody
    @RequestMapping("/delete")
    public JsonResult remove(Long id) {

        try {

            System.out.println("delete id:"+id);

            Map<String, Object> itemMap = itemsService.selectItemById(id);

            if(itemMap != null){
                Integer parentId = (Integer)itemMap.get("parent_id");
                Integer accountType = (Integer)itemMap.get("account_type");

                // 如果是父就先删除子
                if(parentId == 0){
                    itemsService.deleteItemByParentId(id);
                }else if(accountType == 0){
                    return JsonResult.error("GA站点下的广告账户不允许删除");
                }

                itemsService.deleteItemById(id);
            }

            List<Map<String, Object>> gaItemsList = itemsService.selectItemsByType(0);
            List<Map<String, Object>> gaAdAccountList = adAccountService.selectAdAccountByType(0);
//            List<Map<String, Object>> fbAdAccountList = adAccountService.selectAdAccountByType(1);
            List<Map<String, Object>> fbAdAccountList = itemsService.selectItemsCanBind(1);

            return JsonResult.ok("删除成功").put("gaItemsList", JSON.toJSONString(gaItemsList))
                    .put("gaAdAccountList", JSON.toJSONString(gaAdAccountList)).put("fbAdAccountList", JSON.toJSONString(fbAdAccountList));

        } catch (Exception e) {
            e.printStackTrace();
        }

        return JsonResult.error("删除失败");
    }



/*

    @ApiOperation(value = "同步站点")
    @PreAuthorize("hasAuthority('post:/items/sync')")
    @PostMapping("/sync")
*/




    @OperLog(value = "同步数据", desc = "同步站点")
    @RequiresPermissions("sys:items:list")
    @ResponseBody
    @RequestMapping(value = "/sync")
    public JsonResult sync(String json) {

        System.out.println("同步站点："+json);


        try {

            itemsService.syncGoogleItemsData();
            itemsService.syncFacebookItemsData();

            return JsonResult.ok("同步成功");


/*

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
                    itemsService.deleteByType(0);
                    for(AccountSummary item : itemList){
                        String id = item.getId();
                        String name = item.getName();

                        Map map = new HashMap();
                        map.put("id", id);
                        map.put("name", name);
                        map.put("type", 0);

                        System.out.println("map："+map);
                        logger.info("map："+map);
                        itemsService.insertItems(map);
                    }
                }
            }
*/

            // 同步FB站点
/*
            MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
            params.add("access_token", ACCESS_TOKEN);
            params.add("fields", "id,name,account_id");

            String url = GRAPH_URL + BUSINESS_ID + "/client_ad_accounts?";

            System.out.println("url:"+url);

            String fields = "id,name,account_id";
            url = url + "access_token=" + ACCESS_TOKEN + "&fields=" + fields;
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
            List<Map<String, Object>> itemsList = itemsService.selectItems(map0);
            logger.info("itemsLists:" + itemsList.size());

            if(dataArr != null && dataArr.size() > 0){

                itemsService.deleteByType(1);
                for(int i = 0; i < dataArr.size(); i++){

                    JSONObject jsonObj = dataArr.getJSONObject(i);
                    Long id = jsonObj.getLong("account_id");
                    String name = jsonObj.getString("name");

                    Map map = new HashMap();
                    map.put("id", id);
                    map.put("name", name);
                    map.put("type", 1);
                    System.out.println("map:"+map);
                    itemsService.insertItems(map);

                }
            }
*/





        } catch (Exception e) {
            e.printStackTrace();

            logger.error("sync error："+e);

            return JsonResult.error("同步失败");
        }

//        getGaAds();
//        getFBAds();



    }



    @OperLog(value = "同步数据", desc = "Google广告数据")
    @RequiresPermissions("sys:items:list")
    @ResponseBody
    @RequestMapping(value = "/syncGoogleData")
    public JsonResult syncGoogleData(String json) {

        try {
            // 站点数据
            itemsService.syncGoogleItemsData();
            // 广告数据
            adService.syncGoogleData();
            return JsonResult.ok("同步成功");
        } catch (Exception e) {
            e.printStackTrace();
            return JsonResult.error("同步失败");
        }
    }

    @OperLog(value = "同步数据", desc = "Facebook广告数据")
    @RequiresPermissions("sys:items:list")
    @ResponseBody
    @RequestMapping(value = "/syncFacebookData")
    public JsonResult syncFacebookData(String json) {

        try {
            // 站点数据
            itemsService.syncFacebookItemsData();
            // 广告数据
            adService.syncFacebookData();
            return JsonResult.ok("同步成功");
        } catch (Exception e) {
            e.printStackTrace();
            return JsonResult.error("同步失败");
        }
    }

    public void getFBAds() {


        try {

            logger.error("开始同步FB广告啦："+DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss:SSS"));

            // 获取广告账户
            MultiValueMap<String, String> adAccountParams = new LinkedMultiValueMap<>();
            adAccountParams.add("access_token", ACCESS_TOKEN);
            adAccountParams.add("fields", "id,name,account_id,spend");

            String adAccountUrl = GRAPH_URL + BUSINESS_ID + "/client_ad_accounts?";
            logger.error("adAccountParams："+adAccountParams);


            String fields = "id,name,account_id,spend";

            adAccountUrl = adAccountUrl + "access_token=" + ACCESS_TOKEN + "&fields=" + fields;

            logger.error("adAccountUrl："+adAccountUrl);


            String adAccountResult = HttpUtil.get(adAccountUrl, adAccountParams);
//        String adAccountResult = "{\"data\":[{\"id\":\"act_2523536311212491\",\"name\":\"FAYN-MS-dafunia02\",\"account_id\":\"2523536311212491\"},{\"id\":\"act_578026722972031\",\"name\":\"FAYN-MS-dafunia01\",\"account_id\":\"578026722972031\"},{\"id\":\"act_837155403286860\",\"name\":\"FADM-MS-sonsoulier04\",\"account_id\":\"837155403286860\"},{\"id\":\"act_381293539350908\",\"name\":\"FADM-MS-sonsoulier03\",\"account_id\":\"381293539350908\"},{\"id\":\"act_2292495061017464\",\"name\":\"FADM-MS-sonsoulier02\",\"account_id\":\"2292495061017464\"},{\"id\":\"act_532421010599795\",\"name\":\"FADM-MS-sonsoulier01\",\"account_id\":\"532421010599795\"},{\"id\":\"act_2189317301321438\",\"name\":\"FADM-MS-Shoeri03\",\"account_id\":\"2189317301321438\"},{\"id\":\"act_292436271463301\",\"name\":\"FADM-MS-Shoeri04\",\"account_id\":\"292436271463301\"},{\"id\":\"act_231919237755305\",\"name\":\"FADM-MS-Shoeri05\",\"account_id\":\"231919237755305\"},{\"id\":\"act_2160394957354192\",\"name\":\"FADM-MS-Shoeri01\",\"account_id\":\"2160394957354192\"},{\"id\":\"act_1011028935762570\",\"name\":\"FADM-MS-Shoeri02\",\"account_id\":\"1011028935762570\"}],\"paging\":{\"cursors\":{\"before\":\"QVFIUlREOHNaYXpkSV9MRWRjYlREaUR1WnI0ek55VEhwT25CZAlNtczlmeXA1MWJ6MHVtbjBJeEtpN1psZAXVmMmthaVhVUV8zRENSRXB1aDdFUzkxMkZASM0tn\",\"after\":\"QVFIUnNkZA0xnbVBRT2Y2aUJqSjZAlOHVNTDc3Nk4tNEVVSzVzMFNLRFhtaTFFQnJ0b3QtNkV2ejl3cWcxejZARMm5USHlvWVByel9CUWlPQ2VRREwtQjI4V053\"}}}";
            System.out.println("adAccountResult:"+adAccountResult);


            logger.error("adAccountResult："+adAccountResult);


            JSONObject adAccountObject = JSONObject.parseObject(adAccountResult);
            JSONArray adAccountDataArr = adAccountObject.getJSONArray("data");



            // 先删除数据再重新记录
            Map adMap = new HashMap();
            adMap.put("type", 1);
            adMap.put("create_time", DateUtil.timestampToTime(System.currentTimeMillis()-86400000, "yyyy-MM-dd"));
            adService.deleteByType(adMap);

            for(int i = 0; i < adAccountDataArr.size(); i++){

                JSONObject adAccountObj = adAccountDataArr.getJSONObject(i);
                String adAccountId = adAccountObj.getString("account_id");


                // 获取广告系列
                String campaignsUrl = GRAPH_URL + "act_" + adAccountId + "/campaigns?";
                MultiValueMap<String, String> campaignsParams = new LinkedMultiValueMap<>();
                campaignsParams.add("access_token", ACCESS_TOKEN);
                campaignsParams.add("limit", "100");
                campaignsParams.add("fields", "name,start_time,objective,status,spend");


                String campaignsFields = "name,start_time,objective,status,spend";
                campaignsUrl = campaignsUrl + "access_token=" + ACCESS_TOKEN + "&limit=" + 100 + "&fields=" + campaignsFields;
                logger.error("campaignsUrl:"+campaignsUrl);


                String campaignsResult = HttpUtil.get(campaignsUrl, campaignsParams);
                //            String campaignsResult = "{\"data\":[{\"name\":\"SYV1578[110]\",\"start_time\":\"2020-06-10T11:41:33+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"ACTIVE\",\"id\":\"23844857760240787\"},{\"name\":\"SYV1582-df1-YJW\",\"start_time\":\"2020-06-09T11:34:42+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844855998160787\"},{\"name\":\"SYV1571-df1-2-YJW\",\"start_time\":\"2020-06-08T09:31:18+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844850181450787\"},{\"name\":\"SYV1323-testvideo-df1-YJW\",\"start_time\":\"2020-06-06T11:48:19+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844839107080787\"},{\"name\":\"SYV1249-testvideo-df1-YJW\",\"start_time\":\"2020-06-06T11:25:53+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844839037590787\"},{\"name\":\"SYV1571-df1-YJW\",\"start_time\":\"2020-06-05T12:00:30+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844837835820787\"},{\"name\":\"SYV1556[110]\",\"start_time\":\"2020-06-04T11:54:40+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844825691130787\"},{\"name\":\"SYV1526-2-YJW\",\"start_time\":\"2020-06-02T13:51:34+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844819878090787\"},{\"name\":\"SYV1527-2-YJW\",\"start_time\":\"2020-06-02T10:13:27+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844819241890787\"},{\"name\":\"SYV1528-YJW\",\"start_time\":\"2020-05-30T12:00:23+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844807448600787\"},{\"name\":\"SYV1527-YJW\",\"start_time\":\"2020-05-30T11:50:48+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844807428550787\"},{\"name\":\"SYV1524-YJW\",\"start_time\":\"2020-05-30T11:35:03+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844807381730787\"},{\"name\":\"SYV1526-YJW\",\"start_time\":\"2020-05-30T11:27:30+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"ACTIVE\",\"id\":\"23844807361560787\"},{\"name\":\"SYV1225-YJW\",\"start_time\":\"2020-05-30T11:18:02+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844804540840787\"},{\"name\":\"SYV1515-YJW\",\"start_time\":\"2020-05-29T14:26:31+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844802797910787\"},{\"name\":\"SYV1439-YJW\",\"start_time\":\"2020-05-28T10:59:03+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844796492150787\"},{\"name\":\"SYV1487-audience-YJW\",\"start_time\":\"2020-05-26T10:58:14+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844785100060787\"},{\"name\":\"SYV1119-audience-YJW\",\"start_time\":\"2020-05-26T10:29:28+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844784996030787\"},{\"name\":\"SYV1487-YJW\",\"start_time\":\"2020-05-25T10:20:22+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844779987430787\"},{\"name\":\"SYV1436-DF01-3-YJW\",\"start_time\":\"2020-05-23T19:02:38+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844773018940787\"},{\"name\":\"SYV1414-4-YJW\",\"start_time\":\"2020-05-23T19:01:34+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844773013420787\"},{\"name\":\"hity♥0415-2-YJW\",\"start_time\":\"2020-05-19T16:33:44+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844750412530787\"},{\"name\":\"SYV1436-DF01-2-YJW\",\"start_time\":\"2020-05-17T00:51:09+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844739118090787\"},{\"name\":\"SYV1414-3-YJW\",\"start_time\":\"2020-05-16T11:17:38+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844736766270787\"},{\"name\":\"SYV1436-DF01-YJW\",\"start_time\":\"2020-05-16T10:50:15+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844736638860787\"},{\"name\":\"SYV1119-2-YJW\",\"start_time\":\"2020-05-15T17:33:01+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844732440440787\"},{\"name\":\"SYV1414-2-YJW\",\"start_time\":\"2020-05-14T15:48:37+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844725210360787\"},{\"name\":\"SYV1291-0514-YJW\",\"start_time\":\"2020-05-14T10:32:27+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844722827290787\"},{\"name\":\"SYV1414-YJW\",\"start_time\":\"2020-05-14T10:07:32+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844722546820787\"},{\"name\":\"SYV1394-YJW\",\"start_time\":\"2020-05-08T10:37:41+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844686238770787\"},{\"name\":\"SYV1355-YJW\",\"start_time\":\"2020-05-08T10:20:23+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844686223550787\"},{\"name\":\"SYV1325-YJW\",\"start_time\":\"2020-04-26T14:07:59+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844626091150787\"},{\"name\":\"SYV1339-YJW\",\"start_time\":\"2020-04-24T11:05:33+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844612827330787\"},{\"name\":\"YBL0355-YJW\",\"start_time\":\"2020-04-23T11:04:47+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844610970260787\"},{\"name\":\"ubrania-bs-YJW\",\"start_time\":\"2020-04-23T11:49:03+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844610968690787\"},{\"name\":\"SYV1126-新图-YJW\",\"start_time\":\"2020-04-23T11:26:40+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844606548070787\"},{\"name\":\"YBL0332-YJW\",\"start_time\":\"2020-04-22T11:10:21+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844605725000787\"},{\"name\":\"YBL0327-YJW\",\"start_time\":\"2020-04-22T10:54:47+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844605673690787\"},{\"name\":\"YBL0356-YJW\",\"start_time\":\"2020-04-22T10:36:45+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844605603690787\"},{\"name\":\"hity!0421-YJW\",\"start_time\":\"2020-04-21T12:26:35+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844601179180787\"},{\"name\":\"SYV1314-YJW\",\"start_time\":\"2020-04-21T10:28:49+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844598171410787\"},{\"name\":\"SYV1291-2-YJW\",\"start_time\":\"2020-04-20T16:32:56+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844596513750787\"},{\"name\":\"Hot花0420-YJW\",\"start_time\":\"2020-04-20T11:58:18+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844595305680787\"},{\"name\":\"POMOCJA花0420-YJW\",\"start_time\":\"2020-04-20T10:30:57+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844594945680787\"},{\"name\":\"SYV1001-自摄视频-YJW\",\"start_time\":\"2020-04-18T10:44:39+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844579440000787\"},{\"name\":\"SYV1147-2-PL\",\"start_time\":\"2020-04-17T14:26:41+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844579155310787\"},{\"name\":\"SYV1291-YJW\",\"start_time\":\"2020-04-17T14:18:55+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844579134570787\"},{\"name\":\"Hot sale0416-YJW\",\"start_time\":\"2020-04-16T11:18:39+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844571938950787\"},{\"name\":\"SYV1244-2-YJW\",\"start_time\":\"2020-04-16T13:46:43+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844571845500787\"},{\"name\":\"SYV1119-YJW\",\"start_time\":\"2020-04-16T11:10:06+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"ACTIVE\",\"id\":\"23844567962020787\"},{\"name\":\"Kwietnia0415-YJW\",\"start_time\":\"2020-04-15T11:06:47+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844564475070787\"},{\"name\":\"hity♥0415-YJW\",\"start_time\":\"2020-04-15T10:00:07+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844560975160787\"},{\"name\":\"SYV1249-新图新链-YJW\",\"start_time\":\"2020-04-15T09:57:01+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844560890160787\"},{\"name\":\"SYV1147-2-PL\",\"start_time\":\"2020-04-13T10:13:48+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844553516910787\"},{\"name\":\"SYV1147-PL\",\"start_time\":\"2020-04-12T10:19:19+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844546066010787\"},{\"name\":\"SYV1249-video-YJW\",\"start_time\":\"2020-04-10T10:50:48+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844535532070787\"},{\"name\":\"SYZ0674-YJW\",\"start_time\":\"2020-04-10T09:48:22+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844531616940787\"},{\"name\":\"SYV1255-YJW\",\"start_time\":\"2020-04-10T10:09:36+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844531486200787\"},{\"name\":\"SYV0040-0409-YJW\",\"start_time\":\"2020-04-09T14:31:31+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844530949380787\"},{\"name\":\"SYV1253-YJW\",\"start_time\":\"2020-04-09T09:57:48+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844526418920787\"},{\"name\":\"SYV1249-YJW\",\"start_time\":\"2020-04-09T09:47:08+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844526387170787\"},{\"name\":\"SYV1246-YJW\",\"start_time\":\"2020-04-09T10:00:01+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844526309410787\"},{\"name\":\"SYV1244-YJW\",\"start_time\":\"2020-04-09T10:00:37+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844526274330787\"},{\"name\":\"SYV1101-YJW\",\"start_time\":\"2020-04-08T10:15:11+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844521379820787\"},{\"name\":\"SYV0890-YJW\",\"start_time\":\"2020-04-08T10:06:12+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844521344270787\"},{\"name\":\"ubrania-nowosc-0407-YJW\",\"start_time\":\"2020-04-08T12:16:13+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844521259890787\"},{\"name\":\"SYV1180-YJW\",\"start_time\":\"2020-04-03T09:31:16+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844502772150787\"},{\"name\":\"SYV1196-2-YJW\",\"start_time\":\"2020-04-02T11:06:17+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844498259010787\"},{\"name\":\"SYV1047-YJW\",\"start_time\":\"2020-04-02T10:17:20+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844494927940787\"},{\"name\":\"SYV0983-YJW\",\"start_time\":\"2020-04-01T09:53:16+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844493014990787\"},{\"name\":\"SYV0322-YJW\",\"start_time\":\"2020-04-01T10:30:10+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844491336090787\"},{\"name\":\"SYV0944-YJW\",\"start_time\":\"2020-04-01T10:07:51+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844491238050787\"},{\"name\":\"SYV1167-YJW\",\"start_time\":\"2020-03-31T10:47:03+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844488219500787\"},{\"name\":\"SYV1166-YJW\",\"start_time\":\"2020-03-31T10:29:02+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844488152400787\"},{\"name\":\"SYV1190-YJW\",\"start_time\":\"2020-03-31T10:00:32+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844486029680787\"},{\"name\":\"SYV1175-YJW\",\"start_time\":\"2020-03-31T10:00:55+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844484787160787\"},{\"name\":\"SYV1179-2-YJW\",\"start_time\":\"2020-03-30T11:23:48+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844483900590787\"},{\"name\":\"SYV1196-YJW\",\"start_time\":\"2020-03-28T10:00:12+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844473513130787\"},{\"name\":\"OBL03368-YJW\",\"start_time\":\"2020-03-26T11:15:02+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844468583740787\"},{\"name\":\"SYV0976-YJW\",\"start_time\":\"2020-03-26T10:00:50+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844465619580787\"},{\"name\":\"SYV1179-YJW\",\"start_time\":\"2020-03-26T10:00:41+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844465272550787\"},{\"name\":\"SYV1183-YJW\",\"start_time\":\"2020-03-25T10:00:46+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844461741020787\"},{\"name\":\"SYV1187-YJW\",\"start_time\":\"2020-03-25T10:10:39+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844461704760787\"},{\"name\":\"OB0048-YJW\",\"start_time\":\"2020-03-25T10:00:55+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844461600360787\"},{\"name\":\"OB0230-YJW\",\"start_time\":\"2020-03-25T10:00:17+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844461483460787\"},{\"name\":\"OB0269-YJW\",\"start_time\":\"2020-03-24T11:24:46+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844460781000787\"},{\"name\":\"OB0268-YJW\",\"start_time\":\"2020-03-24T11:09:56+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844460720990787\"},{\"name\":\"OBL03358-YJW\",\"start_time\":\"2020-03-23T09:59:37+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844455051760787\"},{\"name\":\"OBL03356-YJW\",\"start_time\":\"2020-03-23T10:59:20+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844455015920787\"},{\"name\":\"OBL03353\",\"start_time\":\"2020-03-22T10:00:00+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844449992660787\"},{\"name\":\"SYV1162-YJW\",\"start_time\":\"2020-03-21T11:09:06+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844449294860787\"},{\"name\":\"SYV0040-YJW\",\"start_time\":\"2020-03-20T10:44:36+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844445118930787\"},{\"name\":\"SYV1153-YJW\",\"start_time\":\"2020-03-20T09:54:11+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844444956060787\"},{\"name\":\"SYV0701-2-YJW\",\"start_time\":\"2020-03-20T09:31:43+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844444906780787\"},{\"name\":\"SYV1116-YJW\",\"start_time\":\"2020-03-20T10:00:07+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844441299210787\"},{\"name\":\"SYV1155-YJW\",\"start_time\":\"2020-03-19T09:34:33+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844440901410787\"},{\"name\":\"SYV1156-YJW\",\"start_time\":\"2020-03-19T10:04:49+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844439277230787\"},{\"name\":\"SYV1154--YJW\",\"start_time\":\"2020-03-19T10:00:45+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844439182980787\"},{\"name\":\"Na wiosnę0317-YJW\",\"start_time\":\"2020-03-18T10:00:38+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844433901770787\"},{\"name\":\"SYV0701-YJW\",\"start_time\":\"2020-03-18T10:00:38+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844433459410787\"}],\"paging\":{\"cursors\":{\"before\":\"QVFIUjhOMDdjTTVTR3JkQXgyNUM2OVNKUjgzLWJmMEt1ZAzA5NkJwWFlqZAW1BY1h3cHhMdTVXR0VIYXFjY0FMVlNjbmhCT0loYjBfQ2JMQTcyb1BmbVI1eHVB\",\"after\":\"QVFIUlkybW9iZAF9yZAzFkcE0zM2drYnBHbWFZAZA0F1RWhHaDlYdUtLcDhCc3ZA2bU4wbmVyTldsWU85ZA1BQUUpNSm5xRUpPTVNpSFBrN2wwczBqM1VaN0dra21R\"},\"next\":\"https://graph.facebook.com/v7.0/act_578026722972031/campaigns?access_token=EAAH92JtasVMBAJ2iHbMXEdLwzMZAH2PidkMGwvQbhFZCZAAcPmUHOxfwaPfNg4M3vXCBonOVZAHLIrj7gdZCJqT9pQs8CAMGrBp7ECuNKOdFIO5txnP3UylNAI959oXBqp1hZAJloEBqSvVdt3hVhXYDu7WGdoZCgZCqrqX0PVE5LKKdGtlzQMxZBmrY8YWjQARUZD&fields=name%2Cstart_time%2Cobjective%2Cstatus%2Ccost_per_conversion%2Cspend&sort=%5B%22name_descending%22%5D&date_preset=this_week_sun_today&limit=100&after=QVFIUlkybW9iZAF9yZAzFkcE0zM2drYnBHbWFZAZA0F1RWhHaDlYdUtLcDhCc3ZA2bU4wbmVyTldsWU85ZA1BQUUpNSm5xRUpPTVNpSFBrN2wwczBqM1VaN0dra21R\"}}";
                System.out.println("campaignsResult:"+campaignsResult);
                logger.error("campaignsResult:"+campaignsResult);


                JSONObject campaignsObjs = JSONObject.parseObject(campaignsResult);
                JSONArray campaignsDataArr = campaignsObjs.getJSONArray("data");
                for(int a = 0; a < campaignsDataArr.size(); a++){
                    JSONObject campaignsObj = campaignsDataArr.getJSONObject(a);
                    String campaignsId = campaignsObj.getString("id");
                    String campaignsName = campaignsObj.getString("name");

                    boolean is = campaignsName.contains("[") && campaignsName.contains("]");
                    if(!is){
                        continue;
                    }

                    // 获取广告系列详情
                    String insightsUrl = GRAPH_URL + campaignsId + "/insights?";
                    MultiValueMap<String, String> insightsParams = new LinkedMultiValueMap<>();
                    insightsParams.add("access_token", ACCESS_TOKEN);
//                    insightsParams.add("fields", "id,name,account_id,spend");
                    String data = DateUtil.timestampToTime(System.currentTimeMillis() - 86400000, "yyyy-MM-dd");
//                    insightsParams.add("time_range", "{'since':'"+data+"','until':'"+data+"'}"); // 查询昨天的

//                    String insightsFields = "id,name,account_id,spend";
                    String time_range = "{'since':'"+data+"','until':'"+data+"'}";
//                    insightsUrl = insightsUrl + "access_token=" + ACCESS_TOKEN + "&time_range=" + time_range;

                    logger.error("insightsUrl2:"+insightsUrl);

                    Map<String,String> params = new HashMap<>();
                    params.put("access_token", ACCESS_TOKEN);
                    params.put("time_range", time_range);
                    params.put("fields", "account_id,campaign_id,impressions,spend,account_name,campaign_name,purchase_roas");

//                    String insightsResult = HttpUtil.get(insightsUrl, insightsParams);
                    String insightsResult = HttpUtil.getInstance().doGet(insightsUrl, params);


                    //                String insightsResult = "{\"data\":[{\"account_id\":\"578026722972031\",\"campaign_id\":\"23844857760240787\",\"impressions\":\"14009\",\"spend\":\"53.06\",\"account_name\":\"FAYN-MS-dafunia01\",\"campaign_name\":\"SYV1578[110]\",\"purchase_roas\":[{\"action_type\":\"omni_purchase\",\"value\":\"3.403694\"}],\"date_start\":\"2020-06-12\",\"date_stop\":\"2020-06-12\"}],\"paging\":{\"cursors\":{\"before\":\"MAZDZD\",\"after\":\"MAZDZD\"}}}";
                    System.out.println("insightsResult:"+insightsResult);

                    logger.error("insightsResult:"+insightsResult);


                    JSONObject insightsObjs = JSONObject.parseObject(insightsResult);
                    JSONArray insightsDataArr = insightsObjs.getJSONArray("data");
                    for(int b = 0; b < insightsDataArr.size(); b++){
                        JSONObject insightsObj = insightsDataArr.getJSONObject(0);
                        Double spend = insightsObj.getDouble("spend"); // 成本
                        String date = insightsObj.getString("date_start");


                        JSONArray purchaseRoasArr = insightsObj.getJSONArray("purchase_roas"); // 花费回报
                        Double value = 0.00;
                        if(purchaseRoasArr != null){
                            JSONObject purchaseRoasObj = purchaseRoasArr.getJSONObject(0);
                            value = purchaseRoasObj.getDouble("value");
                        }


                        Map map = new HashMap<>();
                        map.put("items_id", adAccountId);
                        //                    map.put("profiles_id", profileId);

                        logger.error("campaignsName:"+campaignsName);
                        // 截取广告名称中的工号
                        String result2 = campaignsName.substring(0, campaignsName.indexOf("["));
                        String jobNumber = campaignsName.substring(result2.length()+1, campaignsName.length()-1);

                        map.put("job_number", jobNumber);
                        map.put("ad_account", adAccountId);
                        map.put("ad_name", campaignsName);
                        map.put("source", "facebook.com/cpc"); // 固定不变 写死

                        BigDecimal revenue = new BigDecimal(spend*value).setScale(2, RoundingMode.HALF_UP);
                        map.put("revenue", String.format("%.2f", revenue)); // 收入
                        map.put("cost", spend); // 成本
                        //                    map.put("create_time", DateUtil.timestampToTime(System.currentTimeMillis()-86400000, "yyyy-MM-dd"));
                        map.put("type", 1);
                        map.put("create_time", date);

                        System.out.println("insert map:"+map);

                        adService.insertAd(map);

                    }

                }



            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("itemsTasks2 e:"+e);
        }



    }

    public void getGaAds() {
        System.out.println("定时任务执行时间：" + DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH;mm:ss:SSS"));

        logger.error("记录广告数据 定时任务执行时间：" + DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss:SSS"));


        try {
            Map maps = new HashMap();
            List<Map<String, Object>> certificateList = certificateService.selectAllCertificate(maps);
            System.out.println("certificateList："+certificateList);

            if(certificateList.size() > 0){
                Map<String, Object> certificateMap = certificateList.get(0);
                String serviceAccountId = (String)certificateMap.get("service_account_id");
                String path = (String)certificateMap.get("path");


                File orgFile = new File(File.listRoots()[UploadConstant.UPLOAD_DIS_INDEX], UploadConstant.UPLOAD_DIR + path);

                List<AccountSummary> itemList = AnalyticsUtil.getItems(serviceAccountId, orgFile.getPath());

                System.out.println("itemList:"+itemList);

                String yesterdayDate = DateUtil.timestampToTime(System.currentTimeMillis() - 86400000, "yyyy-MM-dd");
                System.out.println("yesterdayDate："+yesterdayDate);

                // 先删除数据再重新记录
                Map adMap = new HashMap();
                adMap.put("type", 0);
                adMap.put("create_time", DateUtil.timestampToTime(System.currentTimeMillis()-86400000, "yyyy-MM-dd"));
                adService.deleteByType(adMap);

    //            itemsService.deleteAllItems();
                for(AccountSummary item : itemList){
                    String id = item.getId();
                    String name = item.getName();
                    List<WebPropertySummary> webPropertiesList = item.getWebProperties();
                    for(WebPropertySummary WebProperty : webPropertiesList){
                        List<ProfileSummary> profiles = WebProperty.getProfiles();
                        for(ProfileSummary profile : profiles){
                            String profileId = (String)profile.get("id");

                            try {

    //            String adData = AnalyticsUtil.getAdData("206036759", "ostudio01@ostudio01.iam.gserviceaccount.com"
    //                    , "ostudio01-788809f30767.p12", yesterdayDate, yesterdayDate);
                                // 调谷歌接口获取数据
                                String adData = AnalyticsUtil.getAdData(String.valueOf(profileId), serviceAccountId
                                        , orgFile.getPath(), yesterdayDate, yesterdayDate);
                                System.out.println("adData："+adData);

                                logger.error("adTasks adData：" + adData);

                                JSONObject adObj = JSONObject.parseObject(adData);
                                JSONArray rowsArr = adObj.getJSONArray("rows");

                                JSONObject profileInfoObj = adObj.getJSONObject("profileInfo");
                                Long accountId = profileInfoObj.getLong("accountId");

                                if(rowsArr != null && rowsArr.size() > 0){
                                    Map map = new HashMap<>();
                                    for(int i = 0; i < rowsArr.size(); i++){
                                        JSONArray adArr = rowsArr.getJSONArray(i);
                                        String adName = adArr.getString(0); // 广告名称
                                        String adAccount = adArr.getString(1); // 广告账户ID
                                        String source = adArr.getString(2); // 广告渠道
                                        String revenue = adArr.getString(3); // 收入
                                        String cost = adArr.getString(4); // 广告成本

                                        map.put("items_id", accountId);
                                        map.put("profiles_id", profileId);

                                        // 截取广告名称中的工号
                                        String result2 = adName.substring(0, adName.indexOf("["));
                                        String jobNumber = adName.substring(result2.length()+1, adName.length()-1);

                                        map.put("job_number", jobNumber);
                                        map.put("ad_account", adAccount);
                                        map.put("ad_name", adName);
                                        map.put("source", source);
                                        map.put("revenue", revenue);
                                        map.put("cost", cost);
                                        map.put("type", 0);
                                        map.put("create_time", DateUtil.timestampToTime(System.currentTimeMillis()-86400000, "yyyy-MM-dd"));

                                        System.out.println("map:"+map);

                                        adService.insertAd(map);
                                    }
                                }

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        logger.error("GA定时器结束了");
        System.out.println("定时任务结束时间：" + DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH;mm:ss:SSS"));

    }



    /**
     * 获取站点
     */
    @OperLog(value = "站点管理", desc = "获取站点", param = false, result = true)
    @RequiresPermissions("sys:items:list")
    @ResponseBody
    @RequestMapping("/getSite")
    public JsonResult getSite(HttpServletRequest request) {

        System.out.println("获取站点 list："+request);

//        List<Map<String, Object>> itemsList = itemsService.selectItems(map);

//        List<Map<String, Object>> gaList = itemsService.selectItemsByType(0);
//        List<Map<String, Object>> fbList = itemsService.selectItemsByType(1);




/*
        User loginUser = getLoginUser();
        String loginJobNumber = loginUser.getJobNumber();
        List<Role> rolesList = loginUser.getRoles();
        boolean isEmployee = true; // 是否员工
        for(Role  role : rolesList){
            if(role.getRoleId() < 3){
                isEmployee = false;
                break;
            }
        }


        Map map = new HashMap();
        map.put("page", 0);
        map.put("jobNumber", loginJobNumber);
        map.put("rows", 100);
*/


        List<Map<String, Object>> itemList = itemsService.selectItemByParentId(0L);




        JsonResult data = JsonResult.ok().put("data", JSONObject.toJSONString(itemList));
        System.out.println("getSite data2："+JSONObject.toJSON(data));
        return data;

    }





    /**
     * 获取广告账户
     */
    @OperLog(value = "站点管理", desc = "获取广告账户", param = false, result = true)
//    @RequiresPermissions("sys:items:list")
    @ResponseBody
    @RequestMapping("/getAdAccount")
    public JsonResult getADAccount(HttpServletRequest request) {

        System.out.println("站点管理 获取广告账户");
        String itemsIdStr = request.getParameter("itemsId");

        Long itemsId = Long.valueOf(itemsIdStr);
//        List<Map<String, Object>> adAccountList = adService.selectAdGroupByAdAccount(itemsId, jobNumber);

        System.out.println("itemsId:"+itemsId);

//        List<Map<String, Object>> adAccountList = adAccountService.selectAdAccountByItemsId(itemsId);
        List<Map<String, Object>> adAccountList = adService.selectAdAccountGroupByItemsId(itemsId);

        List list = new ArrayList();
        for(Map<String, Object> adAccountMap : adAccountList){

            String value = (String)adAccountMap.get("value");
            String name = (String)adAccountMap.get("name");

            Map map = new HashMap();
            map.put("value", value);
            map.put("name", name);
            map.put("selected", true);
            map.put("disabled", true);
            list.add(map);

        }

        System.out.println("adAccountList:"+adAccountList);
        System.out.println("adAccountList list:"+list);

        JsonResult data = JsonResult.ok().put("data", list);

        return data;
    }


    /**
     * 获取已绑广告账户
     * 修改时调用
     */
    @OperLog(value = "站点管理", desc = "获取已绑广告账户", param = false, result = true)
//    @RequiresPermissions("sys:items:list")
    @ResponseBody
    @RequestMapping("/getBindAdAccount")
    public JsonResult getBindAdAccount(HttpServletRequest request) {

        System.out.println("站点管理 获取已绑广告账户");
        String itemsIdStr = request.getParameter("itemsId");

        Long itemsId = Long.valueOf(itemsIdStr);
        System.out.println("itemsId:"+itemsId);

        // 获取已绑定的GA广告账户
        List<Map<String, Object>> gaAdAccountList = itemsService.selectAdAccountByItemId(itemsId, 0);
        List gaList = new ArrayList();
        for(Map<String, Object> adAccountMap : gaAdAccountList){

            String value = (String)adAccountMap.get("value");
            String name = (String)adAccountMap.get("name");

            Map map = new HashMap();
            map.put("value", value);
            map.put("name", name);
            map.put("selected", true); // GA默认选中并且不可取消
            map.put("disabled", true);
            gaList.add(map);

        }

        // 获取已绑定的FB广告账户
        List<Map<String, Object>> fbAdAccountList = itemsService.selectFBAdAccountByItemId(itemsId, 1);

        List fbList = new ArrayList();
        for(Map<String, Object> adAccountMap : fbAdAccountList){
            String value = (String)adAccountMap.get("value");
            String name = (String)adAccountMap.get("name");

            Map map = new HashMap();
            map.put("value", value);
//            String adAccountName = Cache.getAdAccountName(value); // 获取缓存中的广告账户名称
//            map.put("name", adAccountName != null ? adAccountName : name);
            map.put("name", name);
            map.put("selected", true);
            fbList.add(map);

        }

        System.out.println("fbList2："+JSONArray.toJSONString(fbList));
        // 获取能绑定的FB广告账户
        List<Map<String, Object>> canBindItemsList = itemsService.selectItemsCanBind(1);
        for(Map<String, Object> canBindItemsMap : canBindItemsList){

            String value = (String)canBindItemsMap.get("value");
//            String name = (String)canBindItemsMap.get("name");
            String name = (String)canBindItemsMap.get("name");

            Map map = new HashMap();
            map.put("value", value);
            String adAccountName = Cache.getAdAccountName(String.valueOf(value)); // 获取缓存中的广告账户名称
            map.put("name", adAccountName != null ? adAccountName : name);
//            map.put("selected", true);
//            map.put("disabled", true);
            fbList.add(map);
        }
        System.out.println("fbList2："+JSONArray.toJSONString(fbList));


        JsonResult data = JsonResult.ok().put("gaAdAccountList", gaList).put("fbAdAccountList", fbList);
        System.out.println("获取FB广告账户 data:"+JSONObject.toJSON(data));

        return data;
    }


    /**
     * 获取已绑广告账户
     * 修改时调用
     */
    @OperLog(value = "站点管理", desc = "获取能绑广告账户", param = false, result = true)
//    @RequiresPermissions("sys:items:list")
    @ResponseBody
    @RequestMapping("/getCanBindAdAccount")
    public JsonResult getCanBindAdAccount(HttpServletRequest request) {

        System.out.println("站点管理 获取能绑广告账户");
        String itemsIdStr = request.getParameter("itemsId");

        Long itemsId = Long.valueOf(itemsIdStr);
        System.out.println("itemsId:"+itemsId);

        // 获取已绑定的GA广告账户
//        List<Map<String, Object>> gaAdAccountList = itemsService.selectAdAccountByItemId(itemsId, 0);
        List<Map<String, Object>> gaAdAccountList = adService.selectAdAccountGroupByItemsId(itemsId);
        List gaList = new ArrayList();
        for(Map<String, Object> adAccountMap : gaAdAccountList){

            String value = (String)adAccountMap.get("value");
            String name = (String)adAccountMap.get("name");

            Map map = new HashMap();
            map.put("value", value);
            map.put("name", name);
            map.put("selected", true); // GA默认选中并且不可取消
            map.put("disabled", true);
            gaList.add(map);

        }

        // 获取已绑定的FB广告账户

        List fbList = new ArrayList();
/*
        List<Map<String, Object>> fbAdAccountList = itemsService.selectAdAccountByItemId(itemsId, 1);
        for(Map<String, Object> adAccountMap : fbAdAccountList){

            String value = (String)adAccountMap.get("value");
            String name = (String)adAccountMap.get("name");

            Map map = new HashMap();
            map.put("value", value);
            String adAccountName = Cache.getAdAccountName(value); // 获取缓存中的广告账户名称
            map.put("name", adAccountName != null ? adAccountName : name);
            map.put("selected", true);
//            map.put("disabled", true);
            fbList.add(map);

        }
*/


        // 获取能绑定的FB广告账户
        List<Map<String, Object>> canBindItemsList = itemsService.selectItemsCanBind(1);
        System.out.println("canBindItemsList1："+canBindItemsList);
        for(Map<String, Object> canBindItemsMap : canBindItemsList){

            String value = (String)canBindItemsMap.get("value");
//            String name = (String)canBindItemsMap.get("name");
            String name = (String)canBindItemsMap.get("name");

            Map map = new HashMap();
            map.put("value", value);
            String adAccountName = Cache.getAdAccountName(String.valueOf(value)); // 获取缓存中的广告账户名称
            map.put("name", adAccountName != null ? adAccountName : name);
//            map.put("selected", true);
//            map.put("disabled", true);
            fbList.add(map);
        }

        System.out.println("canBindItemsList2："+fbList);


        JsonResult data = JsonResult.ok().put("gaAdAccountList", gaList).put("fbAdAccountList", fbList);
        System.out.println("获取FB广告账户 data:"+JSONObject.toJSON(data));

        return data;
    }




    /**
     * 获取站点
     */
    @OperLog(value = "站点管理", desc = "获取站点", param = false, result = true)
//    @RequiresPermissions("sys:items:list")
    @ResponseBody
    @RequestMapping("/getItems")
    public JsonResult getItems(HttpServletRequest request) {

        System.out.println("获取站点 getItems："+request);




        Map map = new HashMap();
        map.put("page", 0);
        map.put("jobNumber", "");
        map.put("rows", 100);


        System.out.println("getSite map："+map);
//        List<Map<String, Object>> itemsList = itemsService.selectItems(map);

        List<Map<String, Object>> itemList = itemsService.selectItemsCanBind(0);


//        List<Map<String, Object>> gaList = itemsService.selectItemsByType(0);
//        List<Map<String, Object>> fbList = itemsService.selectItemsByType(1);


        Map m = new HashMap();
        m.put("value", 137727597);
        m.put("name", "Vaschoen");
        itemList.add(m);


        JsonResult data = JsonResult.ok().put("data", JSONObject.toJSONString(itemList));
        System.out.println("getSite data："+JSONObject.toJSON(data));
        return data;
    }


    /**
     * 获取收入
     */
    @OperLog(value = "站点管理", desc = "获取收入", param = false, result = true)
    @RequiresPermissions("sys:items:list")
    @ResponseBody
    @RequestMapping("/getRevenue")
    public JsonResult getRevenue(HttpServletRequest request
            , @RequestParam(name = "month", required = false)String month
            , @RequestParam(name = "itemsId", required = false)Long itemsId) {

        System.out.println("获取收入 month：" + month);
        System.out.println("获取收入 itemsId：" + itemsId);

        Date date = DateUtil.parseDate(month, "yyyy-MM");

        System.out.println("获取收入 date.getTime()：" + date.getTime());

        if(itemsId == null){
            System.out.println("没有itemsId");
            List<Map<String, Object>> itemList = itemsService.selectItemByParentId(0L);
            if(itemList.size() > 0){
                itemsId = (Long)itemList.get(0).get("itemsId");
            }
        }


        Map map = new HashMap();
        map.put("startTime", DateUtil.getAMonthFirstDay(date.getTime()));
        map.put("endTime", DateUtil.getAMonthLastDay(date.getTime()));
        map.put("itemsId", itemsId);

        System.out.println("map："+map);
        Float revenue = adService.selectAMonthRevenue(map);
        System.out.println("revenue："+revenue);

        map = new HashMap();
//        revenue = 1000f;
        map.put("revenue", revenue == null ? 0.00 : revenue);

        JsonResult data = JsonResult.ok().put("data", map);
        System.out.println("getRevenue data2："+JSONObject.toJSON(data));
        return data;

    }




}
