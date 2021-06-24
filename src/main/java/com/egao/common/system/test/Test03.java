package com.egao.common.system.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.utils.DateUtil;
import com.egao.common.core.utils.HttpUtil;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

public class Test03 {

    public static String GRAPH_URL = "https://graph.facebook.com/v7.0/";

    public static String ACCESS_TOKEN = "EAAH92JtasVMBAJ2iHbMXEdLwzMZAH2PidkMGwvQbhFZCZAAcPmUHOxfwaPfNg4M3vXCBonOVZAHLIrj7gdZCJqT9pQs8CAMGrBp7ECuNKOdFIO5txnP3UylNAI959oXBqp1hZAJloEBqSvVdt3hVhXYDu7WGdoZCgZCqrqX0PVE5LKKdGtlzQMxZBmrY8YWjQARUZD";

    public static String BUSINESS_ID = "144436283227029";


    public static void main(String[] args) {

        String adAccountId = "";

        String campaignsResult = "{\"data\":[{\"name\":\"SYV1578-2-[110]\",\"start_time\":\"2020-06-15T16:22:29+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"ACTIVE\",\"id\":\"23844889227940787\"},{\"name\":\"SYV1605-df1-[110]\",\"start_time\":\"2020-06-15T10:59:33+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"ACTIVE\",\"id\":\"23844888067250787\"},{\"name\":\"SYV1578[110]\",\"start_time\":\"2020-06-10T11:41:33+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"ACTIVE\",\"id\":\"23844857760240787\"},{\"name\":\"SYV1582-df1-YJW\",\"start_time\":\"2020-06-09T11:34:42+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844855998160787\"},{\"name\":\"SYV1571-df1-2-YJW\",\"start_time\":\"2020-06-08T09:31:18+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844850181450787\"},{\"name\":\"SYV1323-testvideo-df1-YJW\",\"start_time\":\"2020-06-06T11:48:19+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844839107080787\"},{\"name\":\"SYV1249-testvideo-df1-YJW\",\"start_time\":\"2020-06-06T11:25:53+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844839037590787\"},{\"name\":\"SYV1571-df1-YJW\",\"start_time\":\"2020-06-05T12:00:30+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844837835820787\"},{\"name\":\"SYV1556[110]\",\"start_time\":\"2020-06-04T11:54:40+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844825691130787\"},{\"name\":\"SYV1526-2-YJW\",\"start_time\":\"2020-06-02T13:51:34+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844819878090787\"},{\"name\":\"SYV1527-2-YJW\",\"start_time\":\"2020-06-02T10:13:27+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844819241890787\"},{\"name\":\"SYV1528-YJW\",\"start_time\":\"2020-05-30T12:00:23+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844807448600787\"},{\"name\":\"SYV1527-YJW\",\"start_time\":\"2020-05-30T11:50:48+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844807428550787\"},{\"name\":\"SYV1524-YJW\",\"start_time\":\"2020-05-30T11:35:03+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844807381730787\"},{\"name\":\"SYV1526-YJW\",\"start_time\":\"2020-05-30T11:27:30+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"ACTIVE\",\"id\":\"23844807361560787\"},{\"name\":\"SYV1225-YJW\",\"start_time\":\"2020-05-30T11:18:02+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844804540840787\"},{\"name\":\"SYV1515-YJW\",\"start_time\":\"2020-05-29T14:26:31+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844802797910787\"},{\"name\":\"SYV1439-YJW\",\"start_time\":\"2020-05-28T10:59:03+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844796492150787\"},{\"name\":\"SYV1487-audience-YJW\",\"start_time\":\"2020-05-26T10:58:14+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844785100060787\"},{\"name\":\"SYV1119-audience-YJW\",\"start_time\":\"2020-05-26T10:29:28+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844784996030787\"},{\"name\":\"SYV1487-YJW\",\"start_time\":\"2020-05-25T10:20:22+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844779987430787\"},{\"name\":\"SYV1436-DF01-3-YJW\",\"start_time\":\"2020-05-23T19:02:38+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844773018940787\"},{\"name\":\"SYV1414-4-YJW\",\"start_time\":\"2020-05-23T19:01:34+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844773013420787\"},{\"name\":\"hity\\u26650415-2-YJW\",\"start_time\":\"2020-05-19T16:33:44+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844750412530787\"},{\"name\":\"SYV1436-DF01-2-YJW\",\"start_time\":\"2020-05-17T00:51:09+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844739118090787\"},{\"name\":\"SYV1414-3-YJW\",\"start_time\":\"2020-05-16T11:17:38+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844736766270787\"},{\"name\":\"SYV1436-DF01-YJW\",\"start_time\":\"2020-05-16T10:50:15+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844736638860787\"},{\"name\":\"SYV1119-2-YJW\",\"start_time\":\"2020-05-15T17:33:01+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844732440440787\"},{\"name\":\"SYV1414-2-YJW\",\"start_time\":\"2020-05-14T15:48:37+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844725210360787\"},{\"name\":\"SYV1291-0514-YJW\",\"start_time\":\"2020-05-14T10:32:27+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844722827290787\"},{\"name\":\"SYV1414-YJW\",\"start_time\":\"2020-05-14T10:07:32+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844722546820787\"},{\"name\":\"SYV1394-YJW\",\"start_time\":\"2020-05-08T10:37:41+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844686238770787\"},{\"name\":\"SYV1355-YJW\",\"start_time\":\"2020-05-08T10:20:23+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844686223550787\"},{\"name\":\"SYV1325-YJW\",\"start_time\":\"2020-04-26T14:07:59+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844626091150787\"},{\"name\":\"SYV1339-YJW\",\"start_time\":\"2020-04-24T11:05:33+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844612827330787\"},{\"name\":\"YBL0355-YJW\",\"start_time\":\"2020-04-23T11:04:47+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844610970260787\"},{\"name\":\"ubrania-bs-YJW\",\"start_time\":\"2020-04-23T11:49:03+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844610968690787\"},{\"name\":\"SYV1126-\\u65b0\\u56fe-YJW\",\"start_time\":\"2020-04-23T11:26:40+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844606548070787\"},{\"name\":\"YBL0332-YJW\",\"start_time\":\"2020-04-22T11:10:21+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844605725000787\"},{\"name\":\"YBL0327-YJW\",\"start_time\":\"2020-04-22T10:54:47+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844605673690787\"},{\"name\":\"YBL0356-YJW\",\"start_time\":\"2020-04-22T10:36:45+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844605603690787\"},{\"name\":\"hity!0421-YJW\",\"start_time\":\"2020-04-21T12:26:35+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844601179180787\"},{\"name\":\"SYV1314-YJW\",\"start_time\":\"2020-04-21T10:28:49+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844598171410787\"},{\"name\":\"SYV1291-2-YJW\",\"start_time\":\"2020-04-20T16:32:56+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844596513750787\"},{\"name\":\"Hot\\u82b10420-YJW\",\"start_time\":\"2020-04-20T11:58:18+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844595305680787\"},{\"name\":\"POMOCJA\\u82b10420-YJW\",\"start_time\":\"2020-04-20T10:30:57+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844594945680787\"},{\"name\":\"SYV1001-\\u81ea\\u6444\\u89c6\\u9891-YJW\",\"start_time\":\"2020-04-18T10:44:39+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844579440000787\"},{\"name\":\"SYV1147-2-PL\",\"start_time\":\"2020-04-17T14:26:41+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844579155310787\"},{\"name\":\"SYV1291-YJW\",\"start_time\":\"2020-04-17T14:18:55+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844579134570787\"},{\"name\":\"Hot sale0416-YJW\",\"start_time\":\"2020-04-16T11:18:39+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844571938950787\"},{\"name\":\"SYV1244-2-YJW\",\"start_time\":\"2020-04-16T13:46:43+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844571845500787\"},{\"name\":\"SYV1119-YJW\",\"start_time\":\"2020-04-16T11:10:06+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"ACTIVE\",\"id\":\"23844567962020787\"},{\"name\":\"Kwietnia0415-YJW\",\"start_time\":\"2020-04-15T11:06:47+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844564475070787\"},{\"name\":\"hity\\u26650415-YJW\",\"start_time\":\"2020-04-15T10:00:07+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844560975160787\"},{\"name\":\"SYV1249-\\u65b0\\u56fe\\u65b0\\u94fe-YJW\",\"start_time\":\"2020-04-15T09:57:01+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844560890160787\"},{\"name\":\"SYV1147-2-PL\",\"start_time\":\"2020-04-13T10:13:48+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844553516910787\"},{\"name\":\"SYV1147-PL\",\"start_time\":\"2020-04-12T10:19:19+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844546066010787\"},{\"name\":\"SYV1249-video-YJW\",\"start_time\":\"2020-04-10T10:50:48+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844535532070787\"},{\"name\":\"SYZ0674-YJW\",\"start_time\":\"2020-04-10T09:48:22+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844531616940787\"},{\"name\":\"SYV1255-YJW\",\"start_time\":\"2020-04-10T10:09:36+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844531486200787\"},{\"name\":\"SYV0040-0409-YJW\",\"start_time\":\"2020-04-09T14:31:31+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844530949380787\"},{\"name\":\"SYV1253-YJW\",\"start_time\":\"2020-04-09T09:57:48+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844526418920787\"},{\"name\":\"SYV1249-YJW\",\"start_time\":\"2020-04-09T09:47:08+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844526387170787\"},{\"name\":\"SYV1246-YJW\",\"start_time\":\"2020-04-09T10:00:01+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844526309410787\"},{\"name\":\"SYV1244-YJW\",\"start_time\":\"2020-04-09T10:00:37+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844526274330787\"},{\"name\":\"SYV1101-YJW\",\"start_time\":\"2020-04-08T10:15:11+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844521379820787\"},{\"name\":\"SYV0890-YJW\",\"start_time\":\"2020-04-08T10:06:12+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844521344270787\"},{\"name\":\"ubrania-nowosc-0407-YJW\",\"start_time\":\"2020-04-08T12:16:13+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844521259890787\"},{\"name\":\"SYV1180-YJW\",\"start_time\":\"2020-04-03T09:31:16+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844502772150787\"},{\"name\":\"SYV1196-2-YJW\",\"start_time\":\"2020-04-02T11:06:17+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844498259010787\"},{\"name\":\"SYV1047-YJW\",\"start_time\":\"2020-04-02T10:17:20+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844494927940787\"},{\"name\":\"SYV0983-YJW\",\"start_time\":\"2020-04-01T09:53:16+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844493014990787\"},{\"name\":\"SYV0322-YJW\",\"start_time\":\"2020-04-01T10:30:10+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844491336090787\"},{\"name\":\"SYV0944-YJW\",\"start_time\":\"2020-04-01T10:07:51+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844491238050787\"},{\"name\":\"SYV1167-YJW\",\"start_time\":\"2020-03-31T10:47:03+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844488219500787\"},{\"name\":\"SYV1166-YJW\",\"start_time\":\"2020-03-31T10:29:02+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844488152400787\"},{\"name\":\"SYV1190-YJW\",\"start_time\":\"2020-03-31T10:00:32+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844486029680787\"},{\"name\":\"SYV1175-YJW\",\"start_time\":\"2020-03-31T10:00:55+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844484787160787\"},{\"name\":\"SYV1179-2-YJW\",\"start_time\":\"2020-03-30T11:23:48+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844483900590787\"},{\"name\":\"SYV1196-YJW\",\"start_time\":\"2020-03-28T10:00:12+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844473513130787\"},{\"name\":\"OBL03368-YJW\",\"start_time\":\"2020-03-26T11:15:02+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844468583740787\"},{\"name\":\"SYV0976-YJW\",\"start_time\":\"2020-03-26T10:00:50+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844465619580787\"},{\"name\":\"SYV1179-YJW\",\"start_time\":\"2020-03-26T10:00:41+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844465272550787\"},{\"name\":\"SYV1183-YJW\",\"start_time\":\"2020-03-25T10:00:46+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844461741020787\"},{\"name\":\"SYV1187-YJW\",\"start_time\":\"2020-03-25T10:10:39+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844461704760787\"},{\"name\":\"OB0048-YJW\",\"start_time\":\"2020-03-25T10:00:55+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844461600360787\"},{\"name\":\"OB0230-YJW\",\"start_time\":\"2020-03-25T10:00:17+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844461483460787\"},{\"name\":\"OB0269-YJW\",\"start_time\":\"2020-03-24T11:24:46+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844460781000787\"},{\"name\":\"OB0268-YJW\",\"start_time\":\"2020-03-24T11:09:56+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844460720990787\"},{\"name\":\"OBL03358-YJW\",\"start_time\":\"2020-03-23T09:59:37+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844455051760787\"},{\"name\":\"OBL03356-YJW\",\"start_time\":\"2020-03-23T10:59:20+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844455015920787\"},{\"name\":\"OBL03353\",\"start_time\":\"2020-03-22T10:00:00+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844449992660787\"},{\"name\":\"SYV1162-YJW\",\"start_time\":\"2020-03-21T11:09:06+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844449294860787\"},{\"name\":\"SYV0040-YJW\",\"start_time\":\"2020-03-20T10:44:36+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844445118930787\"},{\"name\":\"SYV1153-YJW\",\"start_time\":\"2020-03-20T09:54:11+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844444956060787\"},{\"name\":\"SYV0701-2-YJW\",\"start_time\":\"2020-03-20T09:31:43+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844444906780787\"},{\"name\":\"SYV1116-YJW\",\"start_time\":\"2020-03-20T10:00:07+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844441299210787\"},{\"name\":\"SYV1155-YJW\",\"start_time\":\"2020-03-19T09:34:33+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844440901410787\"},{\"name\":\"SYV1156-YJW\",\"start_time\":\"2020-03-19T10:04:49+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844439277230787\"},{\"name\":\"SYV1154--YJW\",\"start_time\":\"2020-03-19T10:00:45+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844439182980787\"}],\"paging\":{\"cursors\":{\"before\":\"QVFIUmhlNVJ5bDhLb0IzZAmNTcEdKMGRGZAFFJSnltMVo3RTlDVGw4TlZAkTnJOSlRGN3o0ZAmxKVlhfUGd4OVY1OVdYY3Y0em1DMmFDRl9OUHB4ZAVlUR1VKekFn\",\"after\":\"QVFIUlpNeXY2V3lkR0dacDVndTdQaXFmcXg5cy0zS0dnTDEzVE5URmprZAC1RbkVBc3NoUEd4enlQbjVZAeUVoOVRTbGwtSzl1NjctZAnZAJOVRiTHhCMFdpY1NR\"},\"next\":\"https:\\/\\/graph.facebook.com\\/v7.0\\/act_578026722972031\\/campaigns?access_token=EAAH92JtasVMBAJ2iHbMXEdLwzMZAH2PidkMGwvQbhFZCZAAcPmUHOxfwaPfNg4M3vXCBonOVZAHLIrj7gdZCJqT9pQs8CAMGrBp7ECuNKOdFIO5txnP3UylNAI959oXBqp1hZAJloEBqSvVdt3hVhXYDu7WGdoZCgZCqrqX0PVE5LKKdGtlzQMxZBmrY8YWjQARUZD&fields=name\\u00252Cstart_time\\u00252Cobjective\\u00252Cstatus\\u00252Cspend&limit=100&after=QVFIUlpNeXY2V3lkR0dacDVndTdQaXFmcXg5cy0zS0dnTDEzVE5URmprZAC1RbkVBc3NoUEd4enlQbjVZAeUVoOVRTbGwtSzl1NjctZAnZAJOVRiTHhCMFdpY1NR\"}}";

        int i = 1;


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

//            logger.error("insightsUrl2:"+insightsUrl);

            Map<String,String> params = new HashMap<>();
            params.put("access_token", ACCESS_TOKEN);
            params.put("time_range", time_range);

//                    String insightsResult = HttpUtil.get(insightsUrl, insightsParams);
//            String insightsResult = HttpUtil.getInstance().doGet(insightsUrl, params);
            String insightsResult = "{\"data\":[{\"account_id\":\"578026722972031\",\"campaign_id\":\"23844888067250787\",\"date_start\":\"2020-06-15\",\"date_stop\":\"2020-06-15\",\"impressions\":\"1429\",\"spend\":\"3.92\"}],\"paging\":{\"cursors\":{\"before\":\"MAZDZD\",\"after\":\"MAZDZD\"}}}";


            //                String insightsResult = "{\"data\":[{\"account_id\":\"578026722972031\",\"campaign_id\":\"23844857760240787\",\"impressions\":\"14009\",\"spend\":\"53.06\",\"account_name\":\"FAYN-MS-dafunia01\",\"campaign_name\":\"SYV1578[110]\",\"purchase_roas\":[{\"action_type\":\"omni_purchase\",\"value\":\"3.403694\"}],\"date_start\":\"2020-06-12\",\"date_stop\":\"2020-06-12\"}],\"paging\":{\"cursors\":{\"before\":\"MAZDZD\",\"after\":\"MAZDZD\"}}}";
            System.out.println("insightsResult:"+insightsResult);

//            logger.error("insightsResult:"+insightsResult);


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

                System.out.println("campaignsName："+campaignsName);
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
                map.put("create_time", date);

                System.out.println("..............map:"+map);

//                adService.insertAd(map);

            }

        }






    }
}
