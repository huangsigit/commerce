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
import com.egao.common.system.service.*;
import com.google.api.services.analytics.model.AccountSummary;
import com.google.api.services.analytics.model.ProfileSummary;
import com.google.api.services.analytics.model.WebPropertySummary;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

@Controller
@RequestMapping("/sys/channel")
public class ChannelController extends BaseController {

    private Logger logger = LoggerFactory.getLogger("ChannelController");

    @Autowired
    private ItemsService itemsService;

    @Autowired
    private ChannelService channelService;


    @Autowired
    private AdAccountService adAccountService;


    public static String GRAPH_URL = "https://graph.facebook.com/v7.0/";

    public static String ACCESS_TOKEN = "EAAH92JtasVMBAJ2iHbMXEdLwzMZAH2PidkMGwvQbhFZCZAAcPmUHOxfwaPfNg4M3vXCBonOVZAHLIrj7gdZCJqT9pQs8CAMGrBp7ECuNKOdFIO5txnP3UylNAI959oXBqp1hZAJloEBqSvVdt3hVhXYDu7WGdoZCgZCqrqX0PVE5LKKdGtlzQMxZBmrY8YWjQARUZD";

    public static String BUSINESS_ID = "144436283227029";


    @RequiresPermissions("sys:channel:view")
    @RequestMapping()
    public String view(Model model) {

//        List<Map<String, Object>> gaItemsList = itemsService.selectItemsByType(0);

//        List<Map<String, Object>> fbList = itemsService.selectItemsByType(1);

//        List<Map<String, Object>> gaAdAccountList = adAccountService.selectAdAccountByType(0);
//        List<Map<String, Object>> fbAdAccountList = adAccountService.selectAdAccountByType(1);
        HashMap map = new HashMap();
        map.put("channelId", "");
        List<Map<String, Object>> channelList = channelService.selectCanBindChannel(map);

        System.out.println("channelList："+channelList);

//        model.addAttribute("gaItemsList", JSON.toJSONString(gaItemsList));
//        model.addAttribute("gaAdAccountList", JSON.toJSONString(gaAdAccountList));
//        model.addAttribute("fbAdAccountList", JSON.toJSONString(fbAdAccountList));
//        model.addAttribute("itemList", gaItemsList);
        model.addAttribute("channelList", JSON.toJSONString(channelList));


        return "system/channel.html";
    }



    @OperLog(value = "渠道管理", desc = "分页查询")
    @RequiresPermissions("sys:channel:list")
    @ResponseBody
    @RequestMapping(value = "/page", produces = "application/json;charset=UTF-8")
    public JsonResult list(HttpServletRequest request, @RequestParam(name = "keyword", required = false)String keyword
            , @RequestParam(name = "page", required = false)Integer page, @RequestParam(name = "limit", required = false)Integer limit) {

        System.out.println("渠道管理 start：" + DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH;mm:ss:SSS"));

        System.out.println("page："+page);
        System.out.println("limit："+limit);

        Map map = new HashMap();

//        map.put("page", (page-1)*limit);
        map.put("page", 0);
        map.put("rows", 100);

//        List<Map<String, Object>> itemsList = itemsService.selectItem(map);

        List<Map<String, Object>> channelList = channelService.selectChannel(map);

        int channelCount = 0;
        if(channelList.size() > 0){

            channelCount = channelService.selectChannelCount(map);
        }

        for(Map<String, Object> channelMap : channelList){
            Integer parentId = (Integer)channelMap.get("parent_id");
            String sourceName = (String)channelMap.get("source_name");
            channelMap.put("parentdepartid", parentId);
            channelMap.put("departname", parentId);

/*
            if(parentId != 0){
                    channelMap.put("source_name", "");
//                channelMap.put("adAccountName", adAccountName == null ? adAccount : adAccountName);
            }else{
                channelMap.put("source_name", sourceName);
            }
*/



        }

        JsonResult data = JsonResult.ok(0, channelCount).put("data", channelList);
        System.out.println("渠道管理 list data："+JSONObject.toJSON(data));

        return data;
    }



    @OperLog(value = "渠道管理", desc = "添加数据")
    @RequiresPermissions("sys:channel:add")
    @ResponseBody
    @RequestMapping(value = "/add")
    public JsonResult add(String json, @RequestParam(name = "itemsId", required = false)String itemsId, @RequestParam(name = "sourceName", required = false)String sourceName
            , @RequestParam(name = "channelSel", required = false)String channelSel) {

        try {

            System.out.println("渠道管理 添加数据："+itemsId);
            System.out.println("渠道管理 添加数据 channelSel："+channelSel);

            Map map = new HashMap();

            map.put("parent_id", 0);
            map.put("source_name", sourceName);
            map.put("source", "");
            map.put("type", 0);

            System.out.println("map:"+map);

/*
            if(true){
                return JsonResult.error("添加失败");
            }
*/


//            Long id = itemsService.insertItem(map);
            int id = channelService.insertChannel(map);


/*
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
//                itemsService.updateBindingStatusById(Long.valueOf(gaArr[i]), 1); // 修改绑定状态
            }
                        System.out.println("gaArr："+gaArr);

*/


/*
            String[] fbArr = fbAccountSel.split(",");//注意分隔符是需要转译
            for (int i = 0; i < fbArr.length; i++) {

                map.put("parent_id", id);
                map.put("ad_account", fbArr[i]);
                map.put("item_type", 1);
                map.put("account_type", 1);
                map.put("item_name", "");
//                map.put("item_id", null);
                System.out.println("fb map："+map);
                itemsService.insertItem(map);
            }
*/

            String[] channelArr = channelSel.split(",");//注意分隔符是需要转译
            for (int i = 0; i < channelArr.length; i++) {

                map.put("parent_id", id);
                map.put("source", channelArr[i]);
                map.put("type", 1);
                System.out.println("channel map："+map);
//                itemsService.insertItem(map);
                channelService.insertChannel(map);
            }


            System.out.println("map:"+map);

            HashMap channelMap = new HashMap();
            channelMap.put("channelId", "");
            List<Map<String, Object>> channelList = channelService.selectCanBindChannel(channelMap);

            JsonResult data = JsonResult.ok("添加成功").put("channelList", JSON.toJSONString(channelList));

            System.out.println("渠道管理 data："+JSONObject.toJSON(data));

            return data;
        } catch (Exception e) {
            e.printStackTrace();
            return JsonResult.error("添加失败");
        }
    }



    @OperLog(value = "渠道管理", desc = "修改数据")
    @RequiresPermissions("sys:channel:update")
    @ResponseBody
    @RequestMapping(value = "/update")
    public JsonResult update(String json, @RequestParam(name = "id", required = false)Integer id, @RequestParam(name = "itemsId", required = false)Long itemsId
            , @RequestParam(name = "sourceName", required = false)String sourceName
            , @RequestParam(name = "channelSel", required = false)String channelSel) {

        try {

            System.out.println("站点管理 修改数据 itemsId："+itemsId);
            System.out.println("站点管理 修改数据 fbAccountSel："+channelSel);

            Map map = new HashMap();






/*
            if(true){
                return JsonResult.error("添加失败");
            }
*/


            Map<String, Object> channelMap2 = channelService.selectChannelById(id);
            System.out.println("channelMap2："+channelMap2);
            if(channelMap2 != null){
                Integer parentId = (Integer)channelMap2.get("parent_id");
                // 如果是父
                if(parentId == 0){
//                    itemsService.deleteItemByParentId(id);
                    channelService.deleteByParentId(id);
                    System.out.println("是父："+id);
                }else{ // 如果是子
                    channelService.deleteByParentId(parentId);
                    System.out.println("是子："+parentId);
                    id = parentId;

                }

            }

            map.put("id", id);
            map.put("parent_id", 0);
            map.put("source_name", sourceName);
            map.put("source", "");
            map.put("type", 0);

            System.out.println("map:"+map);

            channelService.updateChannel(map);


//            channelService.deleteByParentId(id);

            String[] channelArr = channelSel.split(",");//注意分隔符是需要转译
            for (int i = 0; i < channelArr.length; i++) {

                map.put("parent_id", id);
                map.put("source_name", sourceName);
                map.put("source", channelArr[i]);
                map.put("type", 1);
                System.out.println("channel map："+map);
//                itemsService.insertItem(map);
                channelService.insertChannel(map);
            }






            System.out.println("map:"+map);

            HashMap channelMap = new HashMap();
            channelMap.put("channelId", "");
            List<Map<String, Object>> channelList = channelService.selectCanBindChannel(channelMap);


            JsonResult data = JsonResult.ok("添加成功").put("channelList", JSON.toJSONString(channelList));

            System.out.println("站点管理 data："+JSONObject.toJSON(data));

            return data;
        } catch (Exception e) {
            e.printStackTrace();
            return JsonResult.error("添加失败");
        }
    }



    /**
     * 删除渠道
     */
    @OperLog(value = "渠道管理", desc = "删除数据", result = true)
    @RequiresPermissions("sys:channel:delete")
    @ResponseBody
    @RequestMapping("/delete")
    public JsonResult remove(Integer id) {

        try {

            System.out.println("delete id:"+id);

//            Map<String, Object> itemMap = itemsService.selectItemById(id);
            Map<String, Object> channelMap = channelService.selectChannelById(id);


            if(channelMap != null){
                Integer parentId = (Integer)channelMap.get("parent_id");

                // 如果是父就先删除子
                if(parentId == 0){
//                    itemsService.deleteItemByParentId(id);
                    channelService.deleteByParentId(id);
                }

//                itemsService.deleteItemById(id);
                channelService.deleteById(id);

                List<Map<String, Object>> channelList = channelService.selectChannelByParentId(parentId);
                if(channelList.size() <= 0){
                    channelService.deleteById(parentId);
                    System.out.println("没有子了："+parentId);

                }
            }

            HashMap channelMap2 = new HashMap();
            channelMap2.put("channelId", "");
            List<Map<String, Object>> channelList = channelService.selectCanBindChannel(channelMap2);

            return JsonResult.ok("删除成功").put("channelList", JSON.toJSONString(channelList));

        } catch (Exception e) {
            e.printStackTrace();
        }

        return JsonResult.error("删除失败");
    }



    /**
     * 获取已绑广告渠道
     * 修改时调用
     */
    @OperLog(value = "渠道管理", desc = "获取能绑广告渠道", param = false, result = true)
//    @RequiresPermissions("sys:items:list")
    @ResponseBody
    @RequestMapping("/getCanBindAdChannel")
    public JsonResult getCanBindAdAccount(HttpServletRequest request) {

        System.out.println("渠道管理 获取能绑广告账户");
        String itemsIdStr = request.getParameter("itemsId");
        String channelIdStr = request.getParameter("channelId");

        System.out.println("channelIdStr:"+channelIdStr);

//        Long itemsId = Long.valueOf(itemsIdStr);
        Integer channelId = Integer.valueOf(channelIdStr);
//        System.out.println("itemsId:"+itemsId);
        System.out.println("channelId:"+channelId);


        // 判断是否子，如果是子就重新赋值channelId
        Map<String, Object> channelMap = channelService.selectChannelById(channelId);
        if(channelMap != null){
            Integer parentId = (Integer)channelMap.get("parent_id");
            channelId = parentId > 0 ? parentId : channelId;
        }



        // 获取已绑定的广告渠道
        Map map = new HashMap();
        map.put("channelId", channelId);
        List<Map<String, Object>> alreadyBindChannelList = channelService.selectCanBindChannel(map);

        System.out.println("alreadyBindChannelList："+JSONArray.toJSONString(alreadyBindChannelList));

        // 获取能绑定的广告渠道
        map.put("channelId", "");
        List<Map<String, Object>> canBindChannelList = channelService.selectCanBindChannel(new HashMap());

        System.out.println("canBindChannelList："+JSONArray.toJSONString(canBindChannelList));

        for(Map<String, Object> alreadyBindChannelMap : alreadyBindChannelList){
            alreadyBindChannelMap.put("selected", true);
        }

        for(Map<String, Object> canBindChannelMap : canBindChannelList){

            alreadyBindChannelList.add(canBindChannelMap);
        }


        System.out.println("alreadyBindChannelList2："+JSONArray.toJSONString(alreadyBindChannelList));




        JsonResult data = JsonResult.ok().put("canBindChannelList", alreadyBindChannelList);

//        data.put("canBindChannelList", JSONArray.parseObject("{\"canBindChannelList\":[{\"name\":\"(direct) / (none)\",\"selected\":true,\"value\":\"(direct) / (none)\"},{\"name\":\"(not set) / product_sync\",\"selected\":true,\"value\":\"(not set) / product_sync\"},{\"name\":\"(not set) / referral\",\"selected\":true,\"value\":\"(not set) / referral\"},{\"name\":\"04kds3cjkjem60gx-24241176640.shopifypreview.com / referral\",\"selected\":true,\"value\":\"04kds3cjkjem60gx-24241176640.shopifypreview.com / referral\"},{\"name\":\"192.168.5.5:802 / referral\",\"selected\":true,\"value\":\"192.168.5.5:802 / referral\"},{\"name\":\"192.168.5.5:805 / referral\",\"selected\":true,\"value\":\"192.168.5.5:805 / referral\"},{\"name\":\"20z.com / referral\",\"selected\":true,\"value\":\"20z.com / referral\"},{\"name\":\"3c-bap.gmx.net / referral\",\"selected\":true,\"value\":\"3c-bap.gmx.net / referral\"},{\"name\":\"423e0531586906b08ef1de5d961da67a.safeframe.googlesyndication.com / referral\",\"selected\":true,\"value\":\"423e0531586906b08ef1de5d961da67a.safeframe.googlesyndication.com / referral\"},{\"name\":\"4b48cd9418caed6263ff73775b6b0850.safeframe.googlesyndication.com / referral\",\"selected\":true,\"value\":\"4b48cd9418caed6263ff73775b6b0850.safeframe.googlesyndication.com / referral\"},{\"name\":\"99reallifestories.com / referral\",\"selected\":true,\"value\":\"99reallifestories.com / referral\"},{\"name\":\"aao-daily.com / referral\",\"selected\":true,\"value\":\"aao-daily.com / referral\"},{\"name\":\"abandoned_cart / email\",\"selected\":true,\"value\":\"abandoned_cart / email\"},{\"name\":\"accounts-oauth.pinterest.com / referral\",\"selected\":true,\"value\":\"accounts-oauth.pinterest.com / referral\"},{\"name\":\"ad / snapchat\",\"selected\":true,\"value\":\"ad / snapchat\"},{\"name\":\"adheart.ru / referral\",\"selected\":true,\"value\":\"adheart.ru / referral\"},{\"name\":\"admin.pearlgo.com / referral\",\"selected\":true,\"value\":\"admin.pearlgo.com / referral\"},{\"name\":\"admin.ycimedia.com / referral\",\"selected\":true,\"value\":\"admin.ycimedia.com / referral\"},{\"name\":\"adreal.gemius.com / referral\",\"selected\":true,\"value\":\"adreal.gemius.com / referral\"},{\"name\":\"ads.us.criteo.com / referral\",\"selected\":true,\"value\":\"ads.us.criteo.com / referral\"},{\"name\":\"adwords.corp.google.com / referral\",\"selected\":true,\"value\":\"adwords.corp.google.com / referral\"},{\"name\":\"aliexpress.com / referral\",\"selected\":true,\"value\":\"aliexpress.com / referral\"},{\"name\":\"alireviews.fireapps.io / referral\",\"selected\":true,\"value\":\"alireviews.fireapps.io / referral\"},{\"name\":\"ampproject.org / referral\",\"selected\":true,\"value\":\"ampproject.org / referral\"},{\"name\":\"animate-iconcept.v2.mein-chat.com / referral\",\"selected\":true,\"value\":\"animate-iconcept.v2.mein-chat.com / referral\"},{\"name\":\"antispam.utc.fr / referral\",\"selected\":true,\"value\":\"antispam.utc.fr / referral\"},{\"name\":\"aol / organic\",\"selected\":true,\"value\":\"aol / organic\"},{\"name\":\"app.activtrak.com / referral\",\"selected\":true,\"value\":\"app.activtrak.com / referral\"},{\"name\":\"app.adspy.com / referral\",\"selected\":true,\"value\":\"app.adspy.com / referral\"},{\"name\":\"app.asana.com / referral\",\"selected\":true,\"value\":\"app.asana.com / referral\"},{\"name\":\"app.buzzsumo.com / referral\",\"selected\":true,\"value\":\"app.buzzsumo.com / referral\"},{\"name\":\"app.kwfinder.com / referral\",\"selected\":true,\"value\":\"app.kwfinder.com / referral\"},{\"name\":\"app.omnisend.com / referral\",\"selected\":true,\"value\":\"app.omnisend.com / referral\"},{\"name\":\"ar.pinterest.com / referral\",\"selected\":true,\"value\":\"ar.pinterest.com / referral\"},{\"name\":\"arrive_app / (not set)\",\"selected\":true,\"value\":\"arrive_app / (not set)\"},{\"name\":\"ask / organic\",\"selected\":true,\"value\":\"ask / organic\"},{\"name\":\"avalon.valsun.cn / referral\",\"selected\":true,\"value\":\"avalon.valsun.cn / referral\"},{\"name\":\"away.vk.com / referral\",\"selected\":true,\"value\":\"away.vk.com / referral\"},{\"name\":\"az78tf52.com / referral\",\"selected\":true,\"value\":\"az78tf52.com / referral\"},{\"name\":\"babylist.com / referral\",\"selected\":true,\"value\":\"babylist.com / referral\"},{\"name\":\"baidu / organic\",\"selected\":true,\"value\":\"baidu / organic\"},{\"name\":\"bcbaebafe_dbeg / email\",\"selected\":true,\"value\":\"bcbaebafe_dbeg / email\"},{\"name\":\"beta.adnalytics.io / referral\",\"selected\":true,\"value\":\"beta.adnalytics.io / referral\"},{\"name\":\"better-selection.com / referral\",\"selected\":true,\"value\":\"better-selection.com / referral\"},{\"name\":\"bing / (not set)\",\"selected\":true,\"value\":\"bing / (not set)\"},{\"name\":\"bing / cpc\",\"selected\":true,\"value\":\"bing / cpc\"},{\"name\":\"bing / organic\",\"selected\":true,\"value\":\"bing / organic\"},{\"name\":\"blankrefer.com / referral\",\"selected\":true,\"value\":\"blankrefer.com / referral\"},{\"name\":\"bnoing.com / referral\",\"selected\":true,\"value\":\"bnoing.com / referral\"},{\"name\":\"br.pinterest.com / referral\",\"selected\":true,\"value\":\"br.pinterest.com / referral\"},{\"name\":\"br.search.yahoo.com / referral\",\"selected\":true,\"value\":\"br.search.yahoo.com / referral\"},{\"name\":\"business.facebook.com / referral\",\"selected\":true,\"value\":\"business.facebook.com / referral\"},{\"name\":\"ca.search.yahoo.com / referral\",\"selected\":true,\"value\":\"ca.search.yahoo.com / referral\"},{\"name\":\"cdn.shopify.com / referral\",\"selected\":true,\"value\":\"cdn.shopify.com / referral\"},{\"name\":\"checkout.us.shopifycs.com / referral\",\"selected\":true,\"value\":\"checkout.us.shopifycs.com / referral\"},{\"name\":\"closetrituals.com / referral\",\"selected\":true,\"value\":\"closetrituals.com / referral\"},{\"name\":\"cn.bing.com / referral\",\"selected\":true,\"value\":\"cn.bing.com / referral\"},{\"name\":\"co.pinterest.com / referral\",\"selected\":true,\"value\":\"co.pinterest.com / referral\"},{\"name\":\"commerceinspector / (not set)\",\"selected\":true,\"value\":\"commerceinspector / (not set)\"},{\"name\":\"connect.webmatic.de / referral\",\"selected\":true,\"value\":\"connect.webmatic.de / referral\"},{\"name\":\"couponifier.com / referral\",\"selected\":true,\"value\":\"couponifier.com / referral\"},{\"name\":\"criteo / (not set)\",\"selected\":true,\"value\":\"criteo / (not set)\"},{\"name\":\"criteo / cpc\",\"selected\":true,\"value\":\"criteo / cpc\"},{\"name\":\"critfeo / cpc\",\"selected\":true,\"value\":\"critfeo / cpc\"},{\"name\":\"crm.xiaoman.cn / referral\",\"selected\":true,\"value\":\"crm.xiaoman.cn / referral\"},{\"name\":\"cse.newshub360.de / referral\",\"selected\":true,\"value\":\"cse.newshub360.de / referral\"},{\"name\":\"curiosone.fr / referral\",\"selected\":true,\"value\":\"curiosone.fr / referral\"},{\"name\":\"cz.pinterest.com / referral\",\"selected\":true,\"value\":\"cz.pinterest.com / referral\"},{\"name\":\"de.search.yahoo.com / referral\",\"selected\":true,\"value\":\"de.search.yahoo.com / referral\"},{\"name\":\"dealspotr.com / referral\",\"selected\":true,\"value\":\"dealspotr.com / referral\"},{\"name\":\"deref-1und1.de / referral\",\"selected\":true,\"value\":\"deref-1und1.de / referral\"},{\"name\":\"deref-gmx.fr / referral\",\"selected\":true,\"value\":\"deref-gmx.fr / referral\"},{\"name\":\"deref-gmx.net / referral\",\"selected\":true,\"value\":\"deref-gmx.net / referral\"},{\"name\":\"deref-mail.com / referral\",\"selected\":true,\"value\":\"deref-mail.com / referral\"},{\"name\":\"deref-web-02.de / referral\",\"selected\":true,\"value\":\"deref-web-02.de / referral\"},{\"name\":\"deref-web.de / referral\",\"selected\":true,\"value\":\"deref-web.de / referral\"},{\"name\":\"dianxiaomi.com / referral\",\"selected\":true,\"value\":\"dianxiaomi.com / referral\"},{\"name\":\"docs.qq.com / referral\",\"selected\":true,\"value\":\"docs.qq.com / referral\"},{\"name\":\"dpcy.net / referral\",\"selected\":true,\"value\":\"dpcy.net / referral\"},{\"name\":\"dropispy.com / referral\",\"selected\":true,\"value\":\"dropispy.com / referral\"},{\"name\":\"dropship.anstrex.com / referral\",\"selected\":true,\"value\":\"dropship.anstrex.com / referral\"},{\"name\":\"duckduckgo / organic\",\"selected\":true,\"value\":\"duckduckgo / organic\"},{\"name\":\"duckduckgo.com / referral\",\"selected\":true,\"value\":\"duckduckgo.com / referral\"},{\"name\":\"ecosia.org / organic\",\"selected\":true,\"value\":\"ecosia.org / organic\"},{\"name\":\"email.bt.com / referral\",\"selected\":true,\"value\":\"email.bt.com / referral\"},{\"name\":\"email.seznam.cz / referral\",\"selected\":true,\"value\":\"email.seznam.cz / referral\"},{\"name\":\"email.t-online.de / referral\",\"selected\":true,\"value\":\"email.t-online.de / referral\"},{\"name\":\"email04.godaddy.com / referral\",\"selected\":true,\"value\":\"email04.godaddy.com / referral\"},{\"name\":\"email06.godaddy.com / referral\",\"selected\":true,\"value\":\"email06.godaddy.com / referral\"},{\"name\":\"emoji.srchmbl.com / referral\",\"selected\":true,\"value\":\"emoji.srchmbl.com / referral\"},{\"name\":\"emonaqueen.com / referral\",\"selected\":true,\"value\":\"emonaqueen.com / referral\"},{\"name\":\"erp.youkeshu.com / referral\",\"selected\":true,\"value\":\"erp.youkeshu.com / referral\"},{\"name\":\"es.search.yahoo.com / referral\",\"selected\":true,\"value\":\"es.search.yahoo.com / referral\"},{\"name\":\"facebook / ads\",\"selected\":true,\"value\":\"facebook / ads\"},{\"name\":\"facebook.com / referral\",\"selected\":true,\"value\":\"facebook.com / referral\"},{\"name\":\"facebook.com/cpc\",\"selected\":true,\"value\":\"facebook.com/cpc\"},{\"name\":\"family-lifeonline.com / referral\",\"selected\":true,\"value\":\"family-lifeonline.com / referral\"},{\"name\":\"fb / (not set)\",\"selected\":true,\"value\":\"fb / (not set)\"},{\"name\":\"fb / cpc\",\"selected\":true,\"value\":\"fb / cpc\"},{\"name\":\"fi.pinterest.com / referral\",\"selected\":true,\"value\":\"fi.pinterest.com / referral\"},{\"name\":\"finder.cox.net / referral\",\"selected\":true,\"value\":\"finder.cox.net / referral\"},{\"name\":\"fr.search.yahoo.com / referral\",\"selected\":true,\"value\":\"fr.search.yahoo.com / referral\"},{\"name\":\"free.facebook.com / referral\",\"selected\":true,\"value\":\"free.facebook.com / referral\"},{\"name\":\"freemail.hu / referral\",\"selected\":true,\"value\":\"freemail.hu / referral\"},{\"name\":\"freemail.net.hr / referral\",\"selected\":true,\"value\":\"freemail.net.hr / referral\"},{\"name\":\"gcp-scm-front-t.vova.com.hk / referral\",\"selected\":true,\"value\":\"gcp-scm-front-t.vova.com.hk / referral\"},{\"name\":\"gioiavinci.com / referral\",\"selected\":true,\"value\":\"gioiavinci.com / referral\"},{\"name\":\"girlsonfilmzine.co.uk / referral\",\"selected\":true,\"value\":\"girlsonfilmzine.co.uk / referral\"},{\"name\":\"gmx.net / referral\",\"selected\":true,\"value\":\"gmx.net / referral\"},{\"name\":\"go / product_sync\",\"selected\":true,\"value\":\"go / product_sync\"},{\"name\":\"go.soarinfotech.com / referral\",\"selected\":true,\"value\":\"go.soarinfotech.com / referral\"},{\"name\":\"google / cpc\",\"selected\":true,\"value\":\"google / cpc\"},{\"name\":\"google / organic\",\"selected\":true,\"value\":\"google / organic\"},{\"name\":\"google / product_sync\",\"selected\":true,\"value\":\"google / product_sync\"},{\"name\":\"google.com / referral\",\"selected\":true,\"value\":\"google.com / referral\"},{\"name\":\"googleads.g.doubleclick.net / referral\",\"selected\":true,\"value\":\"googleads.g.doubleclick.net / referral\"},{\"name\":\"googleweblight.com / referral\",\"selected\":true,\"value\":\"googleweblight.com / referral\"},{\"name\":\"gr.pinterest.com / referral\",\"selected\":true,\"value\":\"gr.pinterest.com / referral\"},{\"name\":\"homedecor / (not set)\",\"selected\":true,\"value\":\"homedecor / (not set)\"},{\"name\":\"hooks.stripe.com / referral\",\"selected\":true,\"value\":\"hooks.stripe.com / referral\"},{\"name\":\"html5.gamedistribution.com / referral\",\"selected\":true,\"value\":\"html5.gamedistribution.com / referral\"},{\"name\":\"hu.pinterest.com / referral\",\"selected\":true,\"value\":\"hu.pinterest.com / referral\"},{\"name\":\"id.pinterest.com / referral\",\"selected\":true,\"value\":\"id.pinterest.com / referral\"},{\"name\":\"images.hk.53yu.com / referral\",\"selected\":true,\"value\":\"images.hk.53yu.com / referral\"},{\"name\":\"imdpm.net / referral\",\"selected\":true,\"value\":\"imdpm.net / referral\"},{\"name\":\"imodel.site / referral\",\"selected\":true,\"value\":\"imodel.site / referral\"},{\"name\":\"in.pinterest.com / referral\",\"selected\":true,\"value\":\"in.pinterest.com / referral\"},{\"name\":\"inbrowserapp.com / referral\",\"selected\":true,\"value\":\"inbrowserapp.com / referral\"},{\"name\":\"info.start.fyi / referral\",\"selected\":true,\"value\":\"info.start.fyi / referral\"},{\"name\":\"instagram.com / referral\",\"selected\":true,\"value\":\"instagram.com / referral\"},{\"name\":\"int.search.tb.ask.com / referral\",\"selected\":true,\"value\":\"int.search.tb.ask.com / referral\"},{\"name\":\"iscoupon.com / referral\",\"selected\":true,\"value\":\"iscoupon.com / referral\"},{\"name\":\"it.search.yahoo.com / referral\",\"selected\":true,\"value\":\"it.search.yahoo.com / referral\"},{\"name\":\"judgeme / email\",\"selected\":true,\"value\":\"judgeme / email\"},{\"name\":\"jwirwp.hxfoot.com / referral\",\"selected\":true,\"value\":\"jwirwp.hxfoot.com / referral\"},{\"name\":\"kapu.hu / referral\",\"selected\":true,\"value\":\"kapu.hu / referral\"},{\"name\":\"kcodqx.pctip.net / referral\",\"selected\":true,\"value\":\"kcodqx.pctip.net / referral\"},{\"name\":\"kdocs.cn / referral\",\"selected\":true,\"value\":\"kdocs.cn / referral\"},{\"name\":\"keep.google.com / referral\",\"selected\":true,\"value\":\"keep.google.com / referral\"},{\"name\":\"knoji.com / referral\",\"selected\":true,\"value\":\"knoji.com / referral\"},{\"name\":\"koala-apps-shopify-inspector / koala-apps-shopify-inspector\",\"selected\":true,\"value\":\"koala-apps-shopify-inspector / koala-apps-shopify-inspector\"},{\"name\":\"l.facebook.com / referral\",\"selected\":true,\"value\":\"l.facebook.com / referral\"},{\"name\":\"l.instagram.com / referral\",\"selected\":true,\"value\":\"l.instagram.com / referral\"},{\"name\":\"laredoute.fr / referral\",\"selected\":true,\"value\":\"laredoute.fr / referral\"},{\"name\":\"lbz.rfesc.net / referral\",\"selected\":true,\"value\":\"lbz.rfesc.net / referral\"},{\"name\":\"lifeloveandcoffeestains.com / referral\",\"selected\":true,\"value\":\"lifeloveandcoffeestains.com / referral\"},{\"name\":\"link.edgepilot.com / referral\",\"selected\":true,\"value\":\"link.edgepilot.com / referral\"},{\"name\":\"listing100.tongtool.com / referral\",\"selected\":true,\"value\":\"listing100.tongtool.com / referral\"},{\"name\":\"lm.facebook.com / referral\",\"selected\":true,\"value\":\"lm.facebook.com / referral\"},{\"name\":\"lsdivas.com / referral\",\"selected\":true,\"value\":\"lsdivas.com / referral\"},{\"name\":\"m-email.t-online.de / referral\",\"selected\":true,\"value\":\"m-email.t-online.de / referral\"},{\"name\":\"m.17track.net / referral\",\"selected\":true,\"value\":\"m.17track.net / referral\"},{\"name\":\"m.abv.bg / referral\",\"selected\":true,\"value\":\"m.abv.bg / referral\"},{\"name\":\"m.facebook.com / referral\",\"selected\":true,\"value\":\"m.facebook.com / referral\"},{\"name\":\"m.vk.com / referral\",\"selected\":true,\"value\":\"m.vk.com / referral\"},{\"name\":\"mail.a1.net / referral\",\"selected\":true,\"value\":\"mail.a1.net / referral\"},{\"name\":\"mail.aol.com / referral\",\"selected\":true,\"value\":\"mail.aol.com / referral\"},{\"name\":\"mail.aol.comah3ngyacp91ux1zhjwajocsumby / referral\",\"selected\":true,\"value\":\"mail.aol.comah3ngyacp91ux1zhjwajocsumby / referral\"},{\"name\":\"mail.aol.comanmekoc7ld7gx2f4ywcmoadwwfy / referral\",\"selected\":true,\"value\":\"mail.aol.comanmekoc7ld7gx2f4ywcmoadwwfy / referral\"},{\"name\":\"mail.centrum.sk / referral\",\"selected\":true,\"value\":\"mail.centrum.sk / referral\"},{\"name\":\"mail.google.com / referral\",\"selected\":true,\"value\":\"mail.google.com / referral\"},{\"name\":\"mail.inbox.lv / referral\",\"selected\":true,\"value\":\"mail.inbox.lv / referral\"},{\"name\":\"mail.vodafone.de / referral\",\"selected\":true,\"value\":\"mail.vodafone.de / referral\"},{\"name\":\"mail.yahoo.com / referral\",\"selected\":true,\"value\":\"mail.yahoo.com / referral\"},{\"name\":\"mail01.orange.fr / referral\",\"selected\":true,\"value\":\"mail01.orange.fr / referral\"},{\"name\":\"mail02.orange.fr / referral\",\"selected\":true,\"value\":\"mail02.orange.fr / referral\"},{\"name\":\"manager.cheetahgo.cmcm.com / referral\",\"selected\":true,\"value\":\"manager.cheetahgo.cmcm.com / referral\"},{\"name\":\"mapolas.com / referral\",\"selected\":true,\"value\":\"mapolas.com / referral\"},{\"name\":\"meetyouattheshow.com / referral\",\"selected\":true,\"value\":\"meetyouattheshow.com / referral\"},{\"name\":\"messageriepro3.orange.fr / referral\",\"selected\":true,\"value\":\"messageriepro3.orange.fr / referral\"},{\"name\":\"messages.google.com / referral\",\"selected\":true,\"value\":\"messages.google.com / referral\"},{\"name\":\"mobilemailer-bap.gmx.net / referral\",\"selected\":true,\"value\":\"mobilemailer-bap.gmx.net / referral\"},{\"name\":\"mobimail.tim.it / referral\",\"selected\":true,\"value\":\"mobimail.tim.it / referral\"},{\"name\":\"msn.com / referral\",\"selected\":true,\"value\":\"msn.com / referral\"},{\"name\":\"mtouch.facebook.com / referral\",\"selected\":true,\"value\":\"mtouch.facebook.com / referral\"},{\"name\":\"my.mail.de / referral\",\"selected\":true,\"value\":\"my.mail.de / referral\"},{\"name\":\"myemail.cox.net / referral\",\"selected\":true,\"value\":\"myemail.cox.net / referral\"},{\"name\":\"mymail.optimum.net / referral\",\"selected\":true,\"value\":\"mymail.optimum.net / referral\"},{\"name\":\"myprivatesearch.com / referral\",\"selected\":true,\"value\":\"myprivatesearch.com / referral\"},{\"name\":\"new.better-selection.com / referral\",\"selected\":true,\"value\":\"new.better-selection.com / referral\"},{\"name\":\"newfbspy.newads.online / referral\",\"selected\":true,\"value\":\"newfbspy.newads.online / referral\"},{\"name\":\"nl.aliexpress.com / referral\",\"selected\":true,\"value\":\"nl.aliexpress.com / referral\"},{\"name\":\"nl.pinterest.com / referral\",\"selected\":true,\"value\":\"nl.pinterest.com / referral\"},{\"name\":\"nl.search.yahoo.com / referral\",\"selected\":true,\"value\":\"nl.search.yahoo.com / referral\"},{\"name\":\"no.pinterest.com / referral\",\"selected\":true,\"value\":\"no.pinterest.com / referral\"},{\"name\":\"obangbag / email\",\"selected\":true,\"value\":\"obangbag / email\"},{\"name\":\"obangbag.knoji.com / referral\",\"selected\":true,\"value\":\"obangbag.knoji.com / referral\"},{\"name\":\"olionana.com / referral\",\"selected\":true,\"value\":\"olionana.com / referral\"},{\"name\":\"omnisend / email\",\"selected\":true,\"value\":\"omnisend / email\"},{\"name\":\"oneone3.co.uk / referral\",\"selected\":true,\"value\":\"oneone3.co.uk / referral\"},{\"name\":\"openurls.com.cn / referral\",\"selected\":true,\"value\":\"openurls.com.cn / referral\"},{\"name\":\"outlook.live.com / referral\",\"selected\":true,\"value\":\"outlook.live.com / referral\"},{\"name\":\"paid.outbrain.com / referral\",\"selected\":true,\"value\":\"paid.outbrain.com / referral\"},{\"name\":\"paperio.site / referral\",\"selected\":true,\"value\":\"paperio.site / referral\"},{\"name\":\"pay-checkout.pingpongx.com / referral\",\"selected\":true,\"value\":\"pay-checkout.pingpongx.com / referral\"},{\"name\":\"paypal.com / referral\",\"selected\":true,\"value\":\"paypal.com / referral\"},{\"name\":\"pinterest / (not set)\",\"selected\":true,\"value\":\"pinterest / (not set)\"},{\"name\":\"pinterest.at / referral\",\"selected\":true,\"value\":\"pinterest.at / referral\"},{\"name\":\"pinterest.ca / referral\",\"selected\":true,\"value\":\"pinterest.ca / referral\"},{\"name\":\"pinterest.ch / referral\",\"selected\":true,\"value\":\"pinterest.ch / referral\"},{\"name\":\"pinterest.cl / referral\",\"selected\":true,\"value\":\"pinterest.cl / referral\"},{\"name\":\"pinterest.co.kr / referral\",\"selected\":true,\"value\":\"pinterest.co.kr / referral\"},{\"name\":\"pinterest.co.uk / referral\",\"selected\":true,\"value\":\"pinterest.co.uk / referral\"},{\"name\":\"pinterest.com / referral\",\"selected\":true,\"value\":\"pinterest.com / referral\"},{\"name\":\"pinterest.com.au / referral\",\"selected\":true,\"value\":\"pinterest.com.au / referral\"},{\"name\":\"pinterest.com.mx / referral\",\"selected\":true,\"value\":\"pinterest.com.mx / referral\"},{\"name\":\"pinterest.de / referral\",\"selected\":true,\"value\":\"pinterest.de / referral\"},{\"name\":\"pinterest.dk / referral\",\"selected\":true,\"value\":\"pinterest.dk / referral\"},{\"name\":\"pinterest.es / referral\",\"selected\":true,\"value\":\"pinterest.es / referral\"},{\"name\":\"pinterest.fr / referral\",\"selected\":true,\"value\":\"pinterest.fr / referral\"},{\"name\":\"pinterest.ie / referral\",\"selected\":true,\"value\":\"pinterest.ie / referral\"},{\"name\":\"pinterest.it / referral\",\"selected\":true,\"value\":\"pinterest.it / referral\"},{\"name\":\"pinterest.jp / referral\",\"selected\":true,\"value\":\"pinterest.jp / referral\"},{\"name\":\"pinterest.nz / referral\",\"selected\":true,\"value\":\"pinterest.nz / referral\"},{\"name\":\"pinterest.ph / referral\",\"selected\":true,\"value\":\"pinterest.ph / referral\"},{\"name\":\"pinterest.pt / referral\",\"selected\":true,\"value\":\"pinterest.pt / referral\"},{\"name\":\"pinterest.ru / referral\",\"selected\":true,\"value\":\"pinterest.ru / referral\"},{\"name\":\"pinterest.se / referral\",\"selected\":true,\"value\":\"pinterest.se / referral\"},{\"name\":\"pl.pinterest.com / referral\",\"selected\":true,\"value\":\"pl.pinterest.com / referral\"},{\"name\":\"pl.search.yahoo.com / referral\",\"selected\":true,\"value\":\"pl.search.yahoo.com / referral\"},{\"name\":\"poczta.o2.pl / referral\",\"selected\":true,\"value\":\"poczta.o2.pl / referral\"},{\"name\":\"poczta.onet.pl / referral\",\"selected\":true,\"value\":\"poczta.onet.pl / referral\"},{\"name\":\"poczta.wp.pl / referral\",\"selected\":true,\"value\":\"poczta.wp.pl / referral\"},{\"name\":\"posti.mail.ee / referral\",\"selected\":true,\"value\":\"posti.mail.ee / referral\"},{\"name\":\"postila.ru / referral\",\"selected\":true,\"value\":\"postila.ru / referral\"},{\"name\":\"pq.pinadmin.com / referral\",\"selected\":true,\"value\":\"pq.pinadmin.com / referral\"},{\"name\":\"prod.uhrs.playmsn.com / referral\",\"selected\":true,\"value\":\"prod.uhrs.playmsn.com / referral\"},{\"name\":\"quickaccess.internet.apps.samsung.com / referral\",\"selected\":true,\"value\":\"quickaccess.internet.apps.samsung.com / referral\"},{\"name\":\"qwant.com / organic\",\"selected\":true,\"value\":\"qwant.com / organic\"},{\"name\":\"r.search.aol.com / referral\",\"selected\":true,\"value\":\"r.search.aol.com / referral\"},{\"name\":\"recherche.aol.fr / referral\",\"selected\":true,\"value\":\"recherche.aol.fr / referral\"},{\"name\":\"retailmenot.com / referral\",\"selected\":true,\"value\":\"retailmenot.com / referral\"},{\"name\":\"review.intern.facebook.com / referral\",\"selected\":true,\"value\":\"review.intern.facebook.com / referral\"},{\"name\":\"ro.pinterest.com / referral\",\"selected\":true,\"value\":\"ro.pinterest.com / referral\"},{\"name\":\"salesource.io / referral\",\"selected\":true,\"value\":\"salesource.io / referral\"},{\"name\":\"scamdoc.com / referral\",\"selected\":true,\"value\":\"scamdoc.com / referral\"},{\"name\":\"scottishbeautyblog.co.uk / referral\",\"selected\":true,\"value\":\"scottishbeautyblog.co.uk / referral\"},{\"name\":\"se.search.yahoo.com / referral\",\"selected\":true,\"value\":\"se.search.yahoo.com / referral\"},{\"name\":\"search-us.com / referral\",\"selected\":true,\"value\":\"search-us.com / referral\"},{\"name\":\"search.aol.com / referral\",\"selected\":true,\"value\":\"search.aol.com / referral\"},{\"name\":\"search.becovi.com / referral\",\"selected\":true,\"value\":\"search.becovi.com / referral\"},{\"name\":\"search.free.fr / referral\",\"selected\":true,\"value\":\"search.free.fr / referral\"},{\"name\":\"search.lilo.org / referral\",\"selected\":true,\"value\":\"search.lilo.org / referral\"},{\"name\":\"search.xfinity.com / referral\",\"selected\":true,\"value\":\"search.xfinity.com / referral\"},{\"name\":\"searchguide.level3.com / referral\",\"selected\":true,\"value\":\"searchguide.level3.com / referral\"},{\"name\":\"secure.oceanpayment.com / referral\",\"selected\":true,\"value\":\"secure.oceanpayment.com / referral\"},{\"name\":\"seller-pulse.com / referral\",\"selected\":true,\"value\":\"seller-pulse.com / referral\"},{\"name\":\"seznam / organic\",\"selected\":true,\"value\":\"seznam / organic\"},{\"name\":\"seznam.cz / referral\",\"selected\":true,\"value\":\"seznam.cz / referral\"},{\"name\":\"shimo.im / referral\",\"selected\":true,\"value\":\"shimo.im / referral\"},{\"name\":\"shopify_email / email\",\"selected\":true,\"value\":\"shopify_email / email\"},{\"name\":\"shopistores.com / referral\",\"selected\":true,\"value\":\"shopistores.com / referral\"},{\"name\":\"shop_app / (not set)\",\"selected\":true,\"value\":\"shop_app / (not set)\"},{\"name\":\"similarsites.com / referral\",\"selected\":true,\"value\":\"similarsites.com / referral\"},{\"name\":\"simplycodes.com / referral\",\"selected\":true,\"value\":\"simplycodes.com / referral\"},{\"name\":\"sk.pinterest.com / referral\",\"selected\":true,\"value\":\"sk.pinterest.com / referral\"},{\"name\":\"skucommon.kokoerp.com / referral\",\"selected\":true,\"value\":\"skucommon.kokoerp.com / referral\"},{\"name\":\"smsbump / sms\",\"selected\":true,\"value\":\"smsbump / sms\"},{\"name\":\"smsbump-automations / sms\",\"selected\":true,\"value\":\"smsbump-automations / sms\"},{\"name\":\"snapchat / (not set)\",\"selected\":true,\"value\":\"snapchat / (not set)\"},{\"name\":\"snapchat.com / referral\",\"selected\":true,\"value\":\"snapchat.com / referral\"},{\"name\":\"so.com / organic\",\"selected\":true,\"value\":\"so.com / organic\"},{\"name\":\"sogou / organic\",\"selected\":true,\"value\":\"sogou / organic\"},{\"name\":\"sogou.com / referral\",\"selected\":true,\"value\":\"sogou.com / referral\"},{\"name\":\"sso.kabelmail.de / referral\",\"selected\":true,\"value\":\"sso.kabelmail.de / referral\"},{\"name\":\"startpage.com / referral\",\"selected\":true,\"value\":\"startpage.com / referral\"},{\"name\":\"startsiden / organic\",\"selected\":true,\"value\":\"startsiden / organic\"},{\"name\":\"stonehouseink.net / referral\",\"selected\":true,\"value\":\"stonehouseink.net / referral\"},{\"name\":\"suche.aol.de / referral\",\"selected\":true,\"value\":\"suche.aol.de / referral\"},{\"name\":\"suche.gmx.net / referral\",\"selected\":true,\"value\":\"suche.gmx.net / referral\"},{\"name\":\"suche.t-online.de / referral\",\"selected\":true,\"value\":\"suche.t-online.de / referral\"},{\"name\":\"suche.web.de / referral\",\"selected\":true,\"value\":\"suche.web.de / referral\"},{\"name\":\"symbaloo.com / referral\",\"selected\":true,\"value\":\"symbaloo.com / referral\"},{\"name\":\"t-online.de / referral\",\"selected\":true,\"value\":\"t-online.de / referral\"},{\"name\":\"t.co / referral\",\"selected\":true,\"value\":\"t.co / referral\"},{\"name\":\"t.post.sme.sk / referral\",\"selected\":true,\"value\":\"t.post.sme.sk / referral\"},{\"name\":\"test-admin.pearlgo.com / referral\",\"selected\":true,\"value\":\"test-admin.pearlgo.com / referral\"},{\"name\":\"tiktok / (not set)\",\"selected\":true,\"value\":\"tiktok / (not set)\"},{\"name\":\"tjs-homes.com / referral\",\"selected\":true,\"value\":\"tjs-homes.com / referral\"},{\"name\":\"tkois.prizesk.com / referral\",\"selected\":true,\"value\":\"tkois.prizesk.com / referral\"},{\"name\":\"touch.facebook.com / referral\",\"selected\":true,\"value\":\"touch.facebook.com / referral\"},{\"name\":\"tr.pinterest.com / referral\",\"selected\":true,\"value\":\"tr.pinterest.com / referral\"},{\"name\":\"translate.googleusercontent.com / referral\",\"selected\":true,\"value\":\"translate.googleusercontent.com / referral\"},{\"name\":\"trendingproducts.com / referral\",\"selected\":true,\"value\":\"trendingproducts.com / referral\"},{\"name\":\"tristablog.online / referral\",\"selected\":true,\"value\":\"tristablog.online / referral\"},{\"name\":\"trustpilot / company_profile\",\"selected\":true,\"value\":\"trustpilot / company_profile\"},{\"name\":\"tulipou.com / referral\",\"selected\":true,\"value\":\"tulipou.com / referral\"},{\"name\":\"ukhpf.pctip.net / referral\",\"selected\":true,\"value\":\"ukhpf.pctip.net / referral\"},{\"name\":\"us.search.yahoo.com / referral\",\"selected\":true,\"value\":\"us.search.yahoo.com / referral\"},{\"name\":\"vickymoda.com / referral\",\"selected\":true,\"value\":\"vickymoda.com / referral\"},{\"name\":\"virgilio / organic\",\"selected\":true,\"value\":\"virgilio / organic\"},{\"name\":\"voice.google.com / referral\",\"selected\":true,\"value\":\"voice.google.com / referral\"},{\"name\":\"wait.la / referral\",\"selected\":true,\"value\":\"wait.la / referral\"},{\"name\":\"web-mail.laposte.net / referral\",\"selected\":true,\"value\":\"web-mail.laposte.net / referral\"},{\"name\":\"web.facebook.com / referral\",\"selected\":true,\"value\":\"web.facebook.com / referral\"},{\"name\":\"webmail.b.earthlink.net / referral\",\"selected\":true,\"value\":\"webmail.b.earthlink.net / referral\"},{\"name\":\"webmail.centurylink.net / referral\",\"selected\":true,\"value\":\"webmail.centurylink.net / referral\"},{\"name\":\"webmail.df.eu / referral\",\"selected\":true,\"value\":\"webmail.df.eu / referral\"},{\"name\":\"webmail.earthlink.net / referral\",\"selected\":true,\"value\":\"webmail.earthlink.net / referral\"},{\"name\":\"webmail.ewe.net / referral\",\"selected\":true,\"value\":\"webmail.ewe.net / referral\"},{\"name\":\"webmail.freenet.de / referral\",\"selected\":true,\"value\":\"webmail.freenet.de / referral\"},{\"name\":\"webmail.hallco.org / referral\",\"selected\":true,\"value\":\"webmail.hallco.org / referral\"},{\"name\":\"webmail.hostingsolutions.it / referral\",\"selected\":true,\"value\":\"webmail.hostingsolutions.it / referral\"},{\"name\":\"webmail.myprincesshouse.com / referral\",\"selected\":true,\"value\":\"webmail.myprincesshouse.com / referral\"},{\"name\":\"webmail.nationaltheater-weimar.de / referral\",\"selected\":true,\"value\":\"webmail.nationaltheater-weimar.de / referral\"},{\"name\":\"webmail.pt.lu / referral\",\"selected\":true,\"value\":\"webmail.pt.lu / referral\"},{\"name\":\"webmail.sfr.fr / referral\",\"selected\":true,\"value\":\"webmail.sfr.fr / referral\"},{\"name\":\"webmail.suddenlink.net / referral\",\"selected\":true,\"value\":\"webmail.suddenlink.net / referral\"},{\"name\":\"webmail.tu-dortmund.de / referral\",\"selected\":true,\"value\":\"webmail.tu-dortmund.de / referral\"},{\"name\":\"webmail.unity-mail.de / referral\",\"selected\":true,\"value\":\"webmail.unity-mail.de / referral\"},{\"name\":\"webmail.windstream.net / referral\",\"selected\":true,\"value\":\"webmail.windstream.net / referral\"},{\"name\":\"webmail02.uoa.gr / referral\",\"selected\":true,\"value\":\"webmail02.uoa.gr / referral\"},{\"name\":\"webmail10a.pc.tim.it / referral\",\"selected\":true,\"value\":\"webmail10a.pc.tim.it / referral\"},{\"name\":\"webmail10c.pc.tim.it / referral\",\"selected\":true,\"value\":\"webmail10c.pc.tim.it / referral\"},{\"name\":\"webmail11e.pc.tim.it / referral\",\"selected\":true,\"value\":\"webmail11e.pc.tim.it / referral\"},{\"name\":\"webmail1c.orange.fr / referral\",\"selected\":true,\"value\":\"webmail1c.orange.fr / referral\"},{\"name\":\"webmail1d.orange.fr / referral\",\"selected\":true,\"value\":\"webmail1d.orange.fr / referral\"},{\"name\":\"webmail1e.orange.fr / referral\",\"selected\":true,\"value\":\"webmail1e.orange.fr / referral\"},{\"name\":\"webmail1f.orange.fr / referral\",\"selected\":true,\"value\":\"webmail1f.orange.fr / referral\"},{\"name\":\"webmail1g.orange.fr / referral\",\"selected\":true,\"value\":\"webmail1g.orange.fr / referral\"},{\"name\":\"webmail1h.orange.fr / referral\",\"selected\":true,\"value\":\"webmail1h.orange.fr / referral\"},{\"name\":\"webmail1j.orange.fr / referral\",\"selected\":true,\"value\":\"webmail1j.orange.fr / referral\"},{\"name\":\"webmail1k.orange.fr / referral\",\"selected\":true,\"value\":\"webmail1k.orange.fr / referral\"},{\"name\":\"webmail1m.orange.fr / referral\",\"selected\":true,\"value\":\"webmail1m.orange.fr / referral\"},{\"name\":\"webmail1n.orange.fr / referral\",\"selected\":true,\"value\":\"webmail1n.orange.fr / referral\"},{\"name\":\"webmail1p.orange.fr / referral\",\"selected\":true,\"value\":\"webmail1p.orange.fr / referral\"},{\"name\":\"webmail2.genevaonline.com / referral\",\"selected\":true,\"value\":\"webmail2.genevaonline.com / referral\"},{\"name\":\"webmail22.orange.fr / referral\",\"selected\":true,\"value\":\"webmail22.orange.fr / referral\"},{\"name\":\"webmail24b.pc.tim.it / referral\",\"selected\":true,\"value\":\"webmail24b.pc.tim.it / referral\"},{\"name\":\"webmail32b.pc.tim.it / referral\",\"selected\":true,\"value\":\"webmail32b.pc.tim.it / referral\"},{\"name\":\"webmail3e.pc.tim.it / referral\",\"selected\":true,\"value\":\"webmail3e.pc.tim.it / referral\"},{\"name\":\"webmaila.juno.com / referral\",\"selected\":true,\"value\":\"webmaila.juno.com / referral\"},{\"name\":\"wmail.orange.fr / referral\",\"selected\":true,\"value\":\"wmail.orange.fr / referral\"},{\"name\":\"womensfashion / (not set)\",\"selected\":true,\"value\":\"womensfashion / (not set)\"},{\"name\":\"ww83.itau.com.br / referral\",\"selected\":true,\"value\":\"ww83.itau.com.br / referral\"},{\"name\":\"www-pinterest-ca.cdn.ampproject.org / referral\",\"selected\":true,\"value\":\"www-pinterest-ca.cdn.ampproject.org / referral\"},{\"name\":\"www-pinterest-com.cdn.ampproject.org / referral\",\"selected\":true,\"value\":\"www-pinterest-com.cdn.ampproject.org / referral\"},{\"name\":\"yahoo / native\",\"selected\":true,\"value\":\"yahoo / native\"},{\"name\":\"yahoo / organic\",\"selected\":true,\"value\":\"yahoo / organic\"},{\"name\":\"yandex / organic\",\"selected\":true,\"value\":\"yandex / organic\"},{\"name\":\"yandex.ru / referral\",\"selected\":true,\"value\":\"yandex.ru / referral\"},{\"name\":\"youtube.com / referral\",\"selected\":true,\"value\":\"youtube.com / referral\"},{\"name\":\"ywh.hxfoot.com / referral\",\"selected\":true,\"value\":\"ywh.hxfoot.com / referral\"},{\"name\":\"za.pinterest.com / referral\",\"selected\":true,\"value\":\"za.pinterest.com / referral\"},{\"name\":\"zimbra.free.fr / referral\",\"selected\":true,\"value\":\"zimbra.free.fr / referral\"},{\"name\":\"(direct) / (none)\",\"value\":\"(direct) / (none)\"},{\"name\":\"(not set) / product_sync\",\"value\":\"(not set) / product_sync\"},{\"name\":\"(not set) / referral\",\"value\":\"(not set) / referral\"},{\"name\":\"04kds3cjkjem60gx-24241176640.shopifypreview.com / referral\",\"value\":\"04kds3cjkjem60gx-24241176640.shopifypreview.com / referral\"},{\"name\":\"192.168.5.5:802 / referral\",\"value\":\"192.168.5.5:802 / referral\"},{\"name\":\"192.168.5.5:805 / referral\",\"value\":\"192.168.5.5:805 / referral\"},{\"name\":\"20z.com / referral\",\"value\":\"20z.com / referral\"},{\"name\":\"3c-bap.gmx.net / referral\",\"value\":\"3c-bap.gmx.net / referral\"},{\"name\":\"423e0531586906b08ef1de5d961da67a.safeframe.googlesyndication.com / referral\",\"value\":\"423e0531586906b08ef1de5d961da67a.safeframe.googlesyndication.com / referral\"},{\"name\":\"4b48cd9418caed6263ff73775b6b0850.safeframe.googlesyndication.com / referral\",\"value\":\"4b48cd9418caed6263ff73775b6b0850.safeframe.googlesyndication.com / referral\"},{\"name\":\"99reallifestories.com / referral\",\"value\":\"99reallifestories.com / referral\"},{\"name\":\"aao-daily.com / referral\",\"value\":\"aao-daily.com / referral\"},{\"name\":\"abandoned_cart / email\",\"value\":\"abandoned_cart / email\"},{\"name\":\"accounts-oauth.pinterest.com / referral\",\"value\":\"accounts-oauth.pinterest.com / referral\"},{\"name\":\"ad / snapchat\",\"value\":\"ad / snapchat\"},{\"name\":\"adheart.ru / referral\",\"value\":\"adheart.ru / referral\"},{\"name\":\"admin.pearlgo.com / referral\",\"value\":\"admin.pearlgo.com / referral\"},{\"name\":\"admin.ycimedia.com / referral\",\"value\":\"admin.ycimedia.com / referral\"},{\"name\":\"adreal.gemius.com / referral\",\"value\":\"adreal.gemius.com / referral\"},{\"name\":\"ads.us.criteo.com / referral\",\"value\":\"ads.us.criteo.com / referral\"},{\"name\":\"adwords.corp.google.com / referral\",\"value\":\"adwords.corp.google.com / referral\"},{\"name\":\"aliexpress.com / referral\",\"value\":\"aliexpress.com / referral\"},{\"name\":\"alireviews.fireapps.io / referral\",\"value\":\"alireviews.fireapps.io / referral\"},{\"name\":\"ampproject.org / referral\",\"value\":\"ampproject.org / referral\"},{\"name\":\"animate-iconcept.v2.mein-chat.com / referral\",\"value\":\"animate-iconcept.v2.mein-chat.com / referral\"},{\"name\":\"antispam.utc.fr / referral\",\"value\":\"antispam.utc.fr / referral\"},{\"name\":\"aol / organic\",\"value\":\"aol / organic\"},{\"name\":\"app.activtrak.com / referral\",\"value\":\"app.activtrak.com / referral\"},{\"name\":\"app.adspy.com / referral\",\"value\":\"app.adspy.com / referral\"},{\"name\":\"app.asana.com / referral\",\"value\":\"app.asana.com / referral\"},{\"name\":\"app.buzzsumo.com / referral\",\"value\":\"app.buzzsumo.com / referral\"},{\"name\":\"app.kwfinder.com / referral\",\"value\":\"app.kwfinder.com / referral\"},{\"name\":\"app.omnisend.com / referral\",\"value\":\"app.omnisend.com / referral\"},{\"name\":\"ar.pinterest.com / referral\",\"value\":\"ar.pinterest.com / referral\"},{\"name\":\"arrive_app / (not set)\",\"value\":\"arrive_app / (not set)\"},{\"name\":\"ask / organic\",\"value\":\"ask / organic\"},{\"name\":\"avalon.valsun.cn / referral\",\"value\":\"avalon.valsun.cn / referral\"},{\"name\":\"away.vk.com / referral\",\"value\":\"away.vk.com / referral\"},{\"name\":\"az78tf52.com / referral\",\"value\":\"az78tf52.com / referral\"},{\"name\":\"babylist.com / referral\",\"value\":\"babylist.com / referral\"},{\"name\":\"baidu / organic\",\"value\":\"baidu / organic\"},{\"name\":\"bcbaebafe_dbeg / email\",\"value\":\"bcbaebafe_dbeg / email\"},{\"name\":\"beta.adnalytics.io / referral\",\"value\":\"beta.adnalytics.io / referral\"},{\"name\":\"better-selection.com / referral\",\"value\":\"better-selection.com / referral\"},{\"name\":\"bing / (not set)\",\"value\":\"bing / (not set)\"},{\"name\":\"bing / cpc\",\"value\":\"bing / cpc\"},{\"name\":\"bing / organic\",\"value\":\"bing / organic\"},{\"name\":\"blankrefer.com / referral\",\"value\":\"blankrefer.com / referral\"},{\"name\":\"bnoing.com / referral\",\"value\":\"bnoing.com / referral\"},{\"name\":\"br.pinterest.com / referral\",\"value\":\"br.pinterest.com / referral\"},{\"name\":\"br.search.yahoo.com / referral\",\"value\":\"br.search.yahoo.com / referral\"},{\"name\":\"business.facebook.com / referral\",\"value\":\"business.facebook.com / referral\"},{\"name\":\"ca.search.yahoo.com / referral\",\"value\":\"ca.search.yahoo.com / referral\"},{\"name\":\"cdn.shopify.com / referral\",\"value\":\"cdn.shopify.com / referral\"},{\"name\":\"checkout.us.shopifycs.com / referral\",\"value\":\"checkout.us.shopifycs.com / referral\"},{\"name\":\"closetrituals.com / referral\",\"value\":\"closetrituals.com / referral\"},{\"name\":\"cn.bing.com / referral\",\"value\":\"cn.bing.com / referral\"},{\"name\":\"co.pinterest.com / referral\",\"value\":\"co.pinterest.com / referral\"},{\"name\":\"commerceinspector / (not set)\",\"value\":\"commerceinspector / (not set)\"},{\"name\":\"connect.webmatic.de / referral\",\"value\":\"connect.webmatic.de / referral\"},{\"name\":\"couponifier.com / referral\",\"value\":\"couponifier.com / referral\"},{\"name\":\"criteo / (not set)\",\"value\":\"criteo / (not set)\"},{\"name\":\"criteo / cpc\",\"value\":\"criteo / cpc\"},{\"name\":\"critfeo / cpc\",\"value\":\"critfeo / cpc\"},{\"name\":\"crm.xiaoman.cn / referral\",\"value\":\"crm.xiaoman.cn / referral\"},{\"name\":\"cse.newshub360.de / referral\",\"value\":\"cse.newshub360.de / referral\"},{\"name\":\"curiosone.fr / referral\",\"value\":\"curiosone.fr / referral\"},{\"name\":\"cz.pinterest.com / referral\",\"value\":\"cz.pinterest.com / referral\"},{\"name\":\"de.search.yahoo.com / referral\",\"value\":\"de.search.yahoo.com / referral\"},{\"name\":\"dealspotr.com / referral\",\"value\":\"dealspotr.com / referral\"},{\"name\":\"deref-1und1.de / referral\",\"value\":\"deref-1und1.de / referral\"},{\"name\":\"deref-gmx.fr / referral\",\"value\":\"deref-gmx.fr / referral\"},{\"name\":\"deref-gmx.net / referral\",\"value\":\"deref-gmx.net / referral\"},{\"name\":\"deref-mail.com / referral\",\"value\":\"deref-mail.com / referral\"},{\"name\":\"deref-web-02.de / referral\",\"value\":\"deref-web-02.de / referral\"},{\"name\":\"deref-web.de / referral\",\"value\":\"deref-web.de / referral\"},{\"name\":\"dianxiaomi.com / referral\",\"value\":\"dianxiaomi.com / referral\"},{\"name\":\"docs.qq.com / referral\",\"value\":\"docs.qq.com / referral\"},{\"name\":\"dpcy.net / referral\",\"value\":\"dpcy.net / referral\"},{\"name\":\"dropispy.com / referral\",\"value\":\"dropispy.com / referral\"},{\"name\":\"dropship.anstrex.com / referral\",\"value\":\"dropship.anstrex.com / referral\"},{\"name\":\"duckduckgo / organic\",\"value\":\"duckduckgo / organic\"},{\"name\":\"duckduckgo.com / referral\",\"value\":\"duckduckgo.com / referral\"},{\"name\":\"ecosia.org / organic\",\"value\":\"ecosia.org / organic\"},{\"name\":\"email.bt.com / referral\",\"value\":\"email.bt.com / referral\"},{\"name\":\"email.seznam.cz / referral\",\"value\":\"email.seznam.cz / referral\"},{\"name\":\"email.t-online.de / referral\",\"value\":\"email.t-online.de / referral\"},{\"name\":\"email04.godaddy.com / referral\",\"value\":\"email04.godaddy.com / referral\"},{\"name\":\"email06.godaddy.com / referral\",\"value\":\"email06.godaddy.com / referral\"},{\"name\":\"emoji.srchmbl.com / referral\",\"value\":\"emoji.srchmbl.com / referral\"},{\"name\":\"emonaqueen.com / referral\",\"value\":\"emonaqueen.com / referral\"},{\"name\":\"erp.youkeshu.com / referral\",\"value\":\"erp.youkeshu.com / referral\"},{\"name\":\"es.search.yahoo.com / referral\",\"value\":\"es.search.yahoo.com / referral\"},{\"name\":\"facebook / ads\",\"value\":\"facebook / ads\"},{\"name\":\"facebook.com / referral\",\"value\":\"facebook.com / referral\"},{\"name\":\"facebook.com/cpc\",\"value\":\"facebook.com/cpc\"},{\"name\":\"family-lifeonline.com / referral\",\"value\":\"family-lifeonline.com / referral\"},{\"name\":\"fb / (not set)\",\"value\":\"fb / (not set)\"},{\"name\":\"fb / cpc\",\"value\":\"fb / cpc\"},{\"name\":\"fi.pinterest.com / referral\",\"value\":\"fi.pinterest.com / referral\"},{\"name\":\"finder.cox.net / referral\",\"value\":\"finder.cox.net / referral\"},{\"name\":\"fr.search.yahoo.com / referral\",\"value\":\"fr.search.yahoo.com / referral\"},{\"name\":\"free.facebook.com / referral\",\"value\":\"free.facebook.com / referral\"},{\"name\":\"freemail.hu / referral\",\"value\":\"freemail.hu / referral\"},{\"name\":\"freemail.net.hr / referral\",\"value\":\"freemail.net.hr / referral\"},{\"name\":\"gcp-scm-front-t.vova.com.hk / referral\",\"value\":\"gcp-scm-front-t.vova.com.hk / referral\"},{\"name\":\"gioiavinci.com / referral\",\"value\":\"gioiavinci.com / referral\"},{\"name\":\"girlsonfilmzine.co.uk / referral\",\"value\":\"girlsonfilmzine.co.uk / referral\"},{\"name\":\"gmx.net / referral\",\"value\":\"gmx.net / referral\"},{\"name\":\"go / product_sync\",\"value\":\"go / product_sync\"},{\"name\":\"go.soarinfotech.com / referral\",\"value\":\"go.soarinfotech.com / referral\"},{\"name\":\"google / cpc\",\"value\":\"google / cpc\"},{\"name\":\"google / organic\",\"value\":\"google / organic\"},{\"name\":\"google / product_sync\",\"value\":\"google / product_sync\"},{\"name\":\"google.com / referral\",\"value\":\"google.com / referral\"},{\"name\":\"googleads.g.doubleclick.net / referral\",\"value\":\"googleads.g.doubleclick.net / referral\"},{\"name\":\"googleweblight.com / referral\",\"value\":\"googleweblight.com / referral\"},{\"name\":\"gr.pinterest.com / referral\",\"value\":\"gr.pinterest.com / referral\"},{\"name\":\"homedecor / (not set)\",\"value\":\"homedecor / (not set)\"},{\"name\":\"hooks.stripe.com / referral\",\"value\":\"hooks.stripe.com / referral\"},{\"name\":\"html5.gamedistribution.com / referral\",\"value\":\"html5.gamedistribution.com / referral\"},{\"name\":\"hu.pinterest.com / referral\",\"value\":\"hu.pinterest.com / referral\"},{\"name\":\"id.pinterest.com / referral\",\"value\":\"id.pinterest.com / referral\"},{\"name\":\"images.hk.53yu.com / referral\",\"value\":\"images.hk.53yu.com / referral\"},{\"name\":\"imdpm.net / referral\",\"value\":\"imdpm.net / referral\"},{\"name\":\"imodel.site / referral\",\"value\":\"imodel.site / referral\"},{\"name\":\"in.pinterest.com / referral\",\"value\":\"in.pinterest.com / referral\"},{\"name\":\"inbrowserapp.com / referral\",\"value\":\"inbrowserapp.com / referral\"},{\"name\":\"info.start.fyi / referral\",\"value\":\"info.start.fyi / referral\"},{\"name\":\"instagram.com / referral\",\"value\":\"instagram.com / referral\"},{\"name\":\"int.search.tb.ask.com / referral\",\"value\":\"int.search.tb.ask.com / referral\"},{\"name\":\"iscoupon.com / referral\",\"value\":\"iscoupon.com / referral\"},{\"name\":\"it.search.yahoo.com / referral\",\"value\":\"it.search.yahoo.com / referral\"},{\"name\":\"judgeme / email\",\"value\":\"judgeme / email\"},{\"name\":\"jwirwp.hxfoot.com / referral\",\"value\":\"jwirwp.hxfoot.com / referral\"},{\"name\":\"kapu.hu / referral\",\"value\":\"kapu.hu / referral\"},{\"name\":\"kcodqx.pctip.net / referral\",\"value\":\"kcodqx.pctip.net / referral\"},{\"name\":\"kdocs.cn / referral\",\"value\":\"kdocs.cn / referral\"},{\"name\":\"keep.google.com / referral\",\"value\":\"keep.google.com / referral\"},{\"name\":\"knoji.com / referral\",\"value\":\"knoji.com / referral\"},{\"name\":\"koala-apps-shopify-inspector / koala-apps-shopify-inspector\",\"value\":\"koala-apps-shopify-inspector / koala-apps-shopify-inspector\"},{\"name\":\"l.facebook.com / referral\",\"value\":\"l.facebook.com / referral\"},{\"name\":\"l.instagram.com / referral\",\"value\":\"l.instagram.com / referral\"},{\"name\":\"laredoute.fr / referral\",\"value\":\"laredoute.fr / referral\"},{\"name\":\"lbz.rfesc.net / referral\",\"value\":\"lbz.rfesc.net / referral\"},{\"name\":\"lifeloveandcoffeestains.com / referral\",\"value\":\"lifeloveandcoffeestains.com / referral\"},{\"name\":\"link.edgepilot.com / referral\",\"value\":\"link.edgepilot.com / referral\"},{\"name\":\"listing100.tongtool.com / referral\",\"value\":\"listing100.tongtool.com / referral\"},{\"name\":\"lm.facebook.com / referral\",\"value\":\"lm.facebook.com / referral\"},{\"name\":\"lsdivas.com / referral\",\"value\":\"lsdivas.com / referral\"},{\"name\":\"m-email.t-online.de / referral\",\"value\":\"m-email.t-online.de / referral\"},{\"name\":\"m.17track.net / referral\",\"value\":\"m.17track.net / referral\"},{\"name\":\"m.abv.bg / referral\",\"value\":\"m.abv.bg / referral\"},{\"name\":\"m.facebook.com / referral\",\"value\":\"m.facebook.com / referral\"},{\"name\":\"m.vk.com / referral\",\"value\":\"m.vk.com / referral\"},{\"name\":\"mail.a1.net / referral\",\"value\":\"mail.a1.net / referral\"},{\"name\":\"mail.aol.com / referral\",\"value\":\"mail.aol.com / referral\"},{\"name\":\"mail.aol.comah3ngyacp91ux1zhjwajocsumby / referral\",\"value\":\"mail.aol.comah3ngyacp91ux1zhjwajocsumby / referral\"},{\"name\":\"mail.aol.comanmekoc7ld7gx2f4ywcmoadwwfy / referral\",\"value\":\"mail.aol.comanmekoc7ld7gx2f4ywcmoadwwfy / referral\"},{\"name\":\"mail.centrum.sk / referral\",\"value\":\"mail.centrum.sk / referral\"},{\"name\":\"mail.google.com / referral\",\"value\":\"mail.google.com / referral\"},{\"name\":\"mail.inbox.lv / referral\",\"value\":\"mail.inbox.lv / referral\"},{\"name\":\"mail.vodafone.de / referral\",\"value\":\"mail.vodafone.de / referral\"},{\"name\":\"mail.yahoo.com / referral\",\"value\":\"mail.yahoo.com / referral\"},{\"name\":\"mail01.orange.fr / referral\",\"value\":\"mail01.orange.fr / referral\"},{\"name\":\"mail02.orange.fr / referral\",\"value\":\"mail02.orange.fr / referral\"},{\"name\":\"manager.cheetahgo.cmcm.com / referral\",\"value\":\"manager.cheetahgo.cmcm.com / referral\"},{\"name\":\"mapolas.com / referral\",\"value\":\"mapolas.com / referral\"},{\"name\":\"meetyouattheshow.com / referral\",\"value\":\"meetyouattheshow.com / referral\"},{\"name\":\"messageriepro3.orange.fr / referral\",\"value\":\"messageriepro3.orange.fr / referral\"},{\"name\":\"messages.google.com / referral\",\"value\":\"messages.google.com / referral\"},{\"name\":\"mobilemailer-bap.gmx.net / referral\",\"value\":\"mobilemailer-bap.gmx.net / referral\"},{\"name\":\"mobimail.tim.it / referral\",\"value\":\"mobimail.tim.it / referral\"},{\"name\":\"msn.com / referral\",\"value\":\"msn.com / referral\"},{\"name\":\"mtouch.facebook.com / referral\",\"value\":\"mtouch.facebook.com / referral\"},{\"name\":\"my.mail.de / referral\",\"value\":\"my.mail.de / referral\"},{\"name\":\"myemail.cox.net / referral\",\"value\":\"myemail.cox.net / referral\"},{\"name\":\"mymail.optimum.net / referral\",\"value\":\"mymail.optimum.net / referral\"},{\"name\":\"myprivatesearch.com / referral\",\"value\":\"myprivatesearch.com / referral\"},{\"name\":\"new.better-selection.com / referral\",\"value\":\"new.better-selection.com / referral\"},{\"name\":\"newfbspy.newads.online / referral\",\"value\":\"newfbspy.newads.online / referral\"},{\"name\":\"nl.aliexpress.com / referral\",\"value\":\"nl.aliexpress.com / referral\"},{\"name\":\"nl.pinterest.com / referral\",\"value\":\"nl.pinterest.com / referral\"},{\"name\":\"nl.search.yahoo.com / referral\",\"value\":\"nl.search.yahoo.com / referral\"},{\"name\":\"no.pinterest.com / referral\",\"value\":\"no.pinterest.com / referral\"},{\"name\":\"obangbag / email\",\"value\":\"obangbag / email\"},{\"name\":\"obangbag.knoji.com / referral\",\"value\":\"obangbag.knoji.com / referral\"},{\"name\":\"olionana.com / referral\",\"value\":\"olionana.com / referral\"},{\"name\":\"omnisend / email\",\"value\":\"omnisend / email\"},{\"name\":\"oneone3.co.uk / referral\",\"value\":\"oneone3.co.uk / referral\"},{\"name\":\"openurls.com.cn / referral\",\"value\":\"openurls.com.cn / referral\"},{\"name\":\"outlook.live.com / referral\",\"value\":\"outlook.live.com / referral\"},{\"name\":\"paid.outbrain.com / referral\",\"value\":\"paid.outbrain.com / referral\"},{\"name\":\"paperio.site / referral\",\"value\":\"paperio.site / referral\"},{\"name\":\"pay-checkout.pingpongx.com / referral\",\"value\":\"pay-checkout.pingpongx.com / referral\"},{\"name\":\"paypal.com / referral\",\"value\":\"paypal.com / referral\"},{\"name\":\"pinterest / (not set)\",\"value\":\"pinterest / (not set)\"},{\"name\":\"pinterest.at / referral\",\"value\":\"pinterest.at / referral\"},{\"name\":\"pinterest.ca / referral\",\"value\":\"pinterest.ca / referral\"},{\"name\":\"pinterest.ch / referral\",\"value\":\"pinterest.ch / referral\"},{\"name\":\"pinterest.cl / referral\",\"value\":\"pinterest.cl / referral\"},{\"name\":\"pinterest.co.kr / referral\",\"value\":\"pinterest.co.kr / referral\"},{\"name\":\"pinterest.co.uk / referral\",\"value\":\"pinterest.co.uk / referral\"},{\"name\":\"pinterest.com / referral\",\"value\":\"pinterest.com / referral\"},{\"name\":\"pinterest.com.au / referral\",\"value\":\"pinterest.com.au / referral\"},{\"name\":\"pinterest.com.mx / referral\",\"value\":\"pinterest.com.mx / referral\"},{\"name\":\"pinterest.de / referral\",\"value\":\"pinterest.de / referral\"},{\"name\":\"pinterest.dk / referral\",\"value\":\"pinterest.dk / referral\"},{\"name\":\"pinterest.es / referral\",\"value\":\"pinterest.es / referral\"},{\"name\":\"pinterest.fr / referral\",\"value\":\"pinterest.fr / referral\"},{\"name\":\"pinterest.ie / referral\",\"value\":\"pinterest.ie / referral\"},{\"name\":\"pinterest.it / referral\",\"value\":\"pinterest.it / referral\"},{\"name\":\"pinterest.jp / referral\",\"value\":\"pinterest.jp / referral\"},{\"name\":\"pinterest.nz / referral\",\"value\":\"pinterest.nz / referral\"},{\"name\":\"pinterest.ph / referral\",\"value\":\"pinterest.ph / referral\"},{\"name\":\"pinterest.pt / referral\",\"value\":\"pinterest.pt / referral\"},{\"name\":\"pinterest.ru / referral\",\"value\":\"pinterest.ru / referral\"},{\"name\":\"pinterest.se / referral\",\"value\":\"pinterest.se / referral\"},{\"name\":\"pl.pinterest.com / referral\",\"value\":\"pl.pinterest.com / referral\"},{\"name\":\"pl.search.yahoo.com / referral\",\"value\":\"pl.search.yahoo.com / referral\"},{\"name\":\"poczta.o2.pl / referral\",\"value\":\"poczta.o2.pl / referral\"},{\"name\":\"poczta.onet.pl / referral\",\"value\":\"poczta.onet.pl / referral\"},{\"name\":\"poczta.wp.pl / referral\",\"value\":\"poczta.wp.pl / referral\"},{\"name\":\"posti.mail.ee / referral\",\"value\":\"posti.mail.ee / referral\"},{\"name\":\"postila.ru / referral\",\"value\":\"postila.ru / referral\"},{\"name\":\"pq.pinadmin.com / referral\",\"value\":\"pq.pinadmin.com / referral\"},{\"name\":\"prod.uhrs.playmsn.com / referral\",\"value\":\"prod.uhrs.playmsn.com / referral\"},{\"name\":\"quickaccess.internet.apps.samsung.com / referral\",\"value\":\"quickaccess.internet.apps.samsung.com / referral\"},{\"name\":\"qwant.com / organic\",\"value\":\"qwant.com / organic\"},{\"name\":\"r.search.aol.com / referral\",\"value\":\"r.search.aol.com / referral\"},{\"name\":\"recherche.aol.fr / referral\",\"value\":\"recherche.aol.fr / referral\"},{\"name\":\"retailmenot.com / referral\",\"value\":\"retailmenot.com / referral\"},{\"name\":\"review.intern.facebook.com / referral\",\"value\":\"review.intern.facebook.com / referral\"},{\"name\":\"ro.pinterest.com / referral\",\"value\":\"ro.pinterest.com / referral\"},{\"name\":\"salesource.io / referral\",\"value\":\"salesource.io / referral\"},{\"name\":\"scamdoc.com / referral\",\"value\":\"scamdoc.com / referral\"},{\"name\":\"scottishbeautyblog.co.uk / referral\",\"value\":\"scottishbeautyblog.co.uk / referral\"},{\"name\":\"se.search.yahoo.com / referral\",\"value\":\"se.search.yahoo.com / referral\"},{\"name\":\"search-us.com / referral\",\"value\":\"search-us.com / referral\"},{\"name\":\"search.aol.com / referral\",\"value\":\"search.aol.com / referral\"},{\"name\":\"search.becovi.com / referral\",\"value\":\"search.becovi.com / referral\"},{\"name\":\"search.free.fr / referral\",\"value\":\"search.free.fr / referral\"},{\"name\":\"search.lilo.org / referral\",\"value\":\"search.lilo.org / referral\"},{\"name\":\"search.xfinity.com / referral\",\"value\":\"search.xfinity.com / referral\"},{\"name\":\"searchguide.level3.com / referral\",\"value\":\"searchguide.level3.com / referral\"},{\"name\":\"secure.oceanpayment.com / referral\",\"value\":\"secure.oceanpayment.com / referral\"},{\"name\":\"seller-pulse.com / referral\",\"value\":\"seller-pulse.com / referral\"},{\"name\":\"seznam / organic\",\"value\":\"seznam / organic\"},{\"name\":\"seznam.cz / referral\",\"value\":\"seznam.cz / referral\"},{\"name\":\"shimo.im / referral\",\"value\":\"shimo.im / referral\"},{\"name\":\"shopify_email / email\",\"value\":\"shopify_email / email\"},{\"name\":\"shopistores.com / referral\",\"value\":\"shopistores.com / referral\"},{\"name\":\"shop_app / (not set)\",\"value\":\"shop_app / (not set)\"},{\"name\":\"similarsites.com / referral\",\"value\":\"similarsites.com / referral\"},{\"name\":\"simplycodes.com / referral\",\"value\":\"simplycodes.com / referral\"},{\"name\":\"sk.pinterest.com / referral\",\"value\":\"sk.pinterest.com / referral\"},{\"name\":\"skucommon.kokoerp.com / referral\",\"value\":\"skucommon.kokoerp.com / referral\"},{\"name\":\"smsbump / sms\",\"value\":\"smsbump / sms\"},{\"name\":\"smsbump-automations / sms\",\"value\":\"smsbump-automations / sms\"},{\"name\":\"snapchat / (not set)\",\"value\":\"snapchat / (not set)\"},{\"name\":\"snapchat.com / referral\",\"value\":\"snapchat.com / referral\"},{\"name\":\"so.com / organic\",\"value\":\"so.com / organic\"},{\"name\":\"sogou / organic\",\"value\":\"sogou / organic\"},{\"name\":\"sogou.com / referral\",\"value\":\"sogou.com / referral\"},{\"name\":\"sso.kabelmail.de / referral\",\"value\":\"sso.kabelmail.de / referral\"},{\"name\":\"startpage.com / referral\",\"value\":\"startpage.com / referral\"},{\"name\":\"startsiden / organic\",\"value\":\"startsiden / organic\"},{\"name\":\"stonehouseink.net / referral\",\"value\":\"stonehouseink.net / referral\"},{\"name\":\"suche.aol.de / referral\",\"value\":\"suche.aol.de / referral\"},{\"name\":\"suche.gmx.net / referral\",\"value\":\"suche.gmx.net / referral\"},{\"name\":\"suche.t-online.de / referral\",\"value\":\"suche.t-online.de / referral\"},{\"name\":\"suche.web.de / referral\",\"value\":\"suche.web.de / referral\"},{\"name\":\"symbaloo.com / referral\",\"value\":\"symbaloo.com / referral\"},{\"name\":\"t-online.de / referral\",\"value\":\"t-online.de / referral\"},{\"name\":\"t.co / referral\",\"value\":\"t.co / referral\"},{\"name\":\"t.post.sme.sk / referral\",\"value\":\"t.post.sme.sk / referral\"},{\"name\":\"test-admin.pearlgo.com / referral\",\"value\":\"test-admin.pearlgo.com / referral\"},{\"name\":\"tiktok / (not set)\",\"value\":\"tiktok / (not set)\"},{\"name\":\"tjs-homes.com / referral\",\"value\":\"tjs-homes.com / referral\"},{\"name\":\"tkois.prizesk.com / referral\",\"value\":\"tkois.prizesk.com / referral\"},{\"name\":\"touch.facebook.com / referral\",\"value\":\"touch.facebook.com / referral\"},{\"name\":\"tr.pinterest.com / referral\",\"value\":\"tr.pinterest.com / referral\"},{\"name\":\"translate.googleusercontent.com / referral\",\"value\":\"translate.googleusercontent.com / referral\"},{\"name\":\"trendingproducts.com / referral\",\"value\":\"trendingproducts.com / referral\"},{\"name\":\"tristablog.online / referral\",\"value\":\"tristablog.online / referral\"},{\"name\":\"trustpilot / company_profile\",\"value\":\"trustpilot / company_profile\"},{\"name\":\"tulipou.com / referral\",\"value\":\"tulipou.com / referral\"},{\"name\":\"ukhpf.pctip.net / referral\",\"value\":\"ukhpf.pctip.net / referral\"},{\"name\":\"us.search.yahoo.com / referral\",\"value\":\"us.search.yahoo.com / referral\"},{\"name\":\"vickymoda.com / referral\",\"value\":\"vickymoda.com / referral\"},{\"name\":\"virgilio / organic\",\"value\":\"virgilio / organic\"},{\"name\":\"voice.google.com / referral\",\"value\":\"voice.google.com / referral\"},{\"name\":\"wait.la / referral\",\"value\":\"wait.la / referral\"},{\"name\":\"web-mail.laposte.net / referral\",\"value\":\"web-mail.laposte.net / referral\"},{\"name\":\"web.facebook.com / referral\",\"value\":\"web.facebook.com / referral\"},{\"name\":\"webmail.b.earthlink.net / referral\",\"value\":\"webmail.b.earthlink.net / referral\"},{\"name\":\"webmail.centurylink.net / referral\",\"value\":\"webmail.centurylink.net / referral\"},{\"name\":\"webmail.df.eu / referral\",\"value\":\"webmail.df.eu / referral\"},{\"name\":\"webmail.earthlink.net / referral\",\"value\":\"webmail.earthlink.net / referral\"},{\"name\":\"webmail.ewe.net / referral\",\"value\":\"webmail.ewe.net / referral\"},{\"name\":\"webmail.freenet.de / referral\",\"value\":\"webmail.freenet.de / referral\"},{\"name\":\"webmail.hallco.org / referral\",\"value\":\"webmail.hallco.org / referral\"},{\"name\":\"webmail.hostingsolutions.it / referral\",\"value\":\"webmail.hostingsolutions.it / referral\"},{\"name\":\"webmail.myprincesshouse.com / referral\",\"value\":\"webmail.myprincesshouse.com / referral\"},{\"name\":\"webmail.nationaltheater-weimar.de / referral\",\"value\":\"webmail.nationaltheater-weimar.de / referral\"},{\"name\":\"webmail.pt.lu / referral\",\"value\":\"webmail.pt.lu / referral\"},{\"name\":\"webmail.sfr.fr / referral\",\"value\":\"webmail.sfr.fr / referral\"},{\"name\":\"webmail.suddenlink.net / referral\",\"value\":\"webmail.suddenlink.net / referral\"},{\"name\":\"webmail.tu-dortmund.de / referral\",\"value\":\"webmail.tu-dortmund.de / referral\"},{\"name\":\"webmail.unity-mail.de / referral\",\"value\":\"webmail.unity-mail.de / referral\"},{\"name\":\"webmail.windstream.net / referral\",\"value\":\"webmail.windstream.net / referral\"},{\"name\":\"webmail02.uoa.gr / referral\",\"value\":\"webmail02.uoa.gr / referral\"},{\"name\":\"webmail10a.pc.tim.it / referral\",\"value\":\"webmail10a.pc.tim.it / referral\"},{\"name\":\"webmail10c.pc.tim.it / referral\",\"value\":\"webmail10c.pc.tim.it / referral\"},{\"name\":\"webmail11e.pc.tim.it / referral\",\"value\":\"webmail11e.pc.tim.it / referral\"},{\"name\":\"webmail1c.orange.fr / referral\",\"value\":\"webmail1c.orange.fr / referral\"},{\"name\":\"webmail1d.orange.fr / referral\",\"value\":\"webmail1d.orange.fr / referral\"},{\"name\":\"webmail1e.orange.fr / referral\",\"value\":\"webmail1e.orange.fr / referral\"},{\"name\":\"webmail1f.orange.fr / referral\",\"value\":\"webmail1f.orange.fr / referral\"},{\"name\":\"webmail1g.orange.fr / referral\",\"value\":\"webmail1g.orange.fr / referral\"},{\"name\":\"webmail1h.orange.fr / referral\",\"value\":\"webmail1h.orange.fr / referral\"},{\"name\":\"webmail1j.orange.fr / referral\",\"value\":\"webmail1j.orange.fr / referral\"},{\"name\":\"webmail1k.orange.fr / referral\",\"value\":\"webmail1k.orange.fr / referral\"},{\"name\":\"webmail1m.orange.fr / referral\",\"value\":\"webmail1m.orange.fr / referral\"},{\"name\":\"webmail1n.orange.fr / referral\",\"value\":\"webmail1n.orange.fr / referral\"},{\"name\":\"webmail1p.orange.fr / referral\",\"value\":\"webmail1p.orange.fr / referral\"},{\"name\":\"webmail2.genevaonline.com / referral\",\"value\":\"webmail2.genevaonline.com / referral\"},{\"name\":\"webmail22.orange.fr / referral\",\"value\":\"webmail22.orange.fr / referral\"},{\"name\":\"webmail24b.pc.tim.it / referral\",\"value\":\"webmail24b.pc.tim.it / referral\"},{\"name\":\"webmail32b.pc.tim.it / referral\",\"value\":\"webmail32b.pc.tim.it / referral\"},{\"name\":\"webmail3e.pc.tim.it / referral\",\"value\":\"webmail3e.pc.tim.it / referral\"},{\"name\":\"webmaila.juno.com / referral\",\"value\":\"webmaila.juno.com / referral\"},{\"name\":\"wmail.orange.fr / referral\",\"value\":\"wmail.orange.fr / referral\"},{\"name\":\"womensfashion / (not set)\",\"value\":\"womensfashion / (not set)\"},{\"name\":\"ww83.itau.com.br / referral\",\"value\":\"ww83.itau.com.br / referral\"},{\"name\":\"www-pinterest-ca.cdn.ampproject.org / referral\",\"value\":\"www-pinterest-ca.cdn.ampproject.org / referral\"},{\"name\":\"www-pinterest-com.cdn.ampproject.org / referral\",\"value\":\"www-pinterest-com.cdn.ampproject.org / referral\"},{\"name\":\"yahoo / native\",\"value\":\"yahoo / native\"},{\"name\":\"yahoo / organic\",\"value\":\"yahoo / organic\"},{\"name\":\"yandex / organic\",\"value\":\"yandex / organic\"},{\"name\":\"yandex.ru / referral\",\"value\":\"yandex.ru / referral\"},{\"name\":\"youtube.com / referral\",\"value\":\"youtube.com / referral\"},{\"name\":\"ywh.hxfoot.com / referral\",\"value\":\"ywh.hxfoot.com / referral\"},{\"name\":\"za.pinterest.com / referral\",\"value\":\"za.pinterest.com / referral\"},{\"name\":\"zimbra.free.fr / referral\",\"value\":\"zimbra.free.fr / referral\"}],\"code\":0,\"msg\":\"操作成功\"}"));


//        data.setData("{\"name\":\"04kds3cjkjem60gx-24241176640.shopifypreview.com / referral\",\"selected\":true,\"value\":\"04kds3cjkjem60gx-24241176640.shopifypreview.com / referral\"}");
        System.out.println("获取能绑渠道 data:"+JSONObject.toJSON(data));

        return data;
    }



}
