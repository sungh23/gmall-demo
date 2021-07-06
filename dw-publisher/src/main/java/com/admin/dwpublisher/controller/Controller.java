package com.admin.dwpublisher.controller;

import com.admin.dwpublisher.service.PublisherService;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * @author sungaohua
 */
@RestController
public class Controller {

    @Autowired
    private PublisherService publisherService;


    @RequestMapping("/realtime-total")
    public String selectDauTotal(@RequestParam("date") String date) {

        //1.获取service层处理后的数据
        Integer dauTotal = publisherService.getDauTotal(date);
        Double amountTotal = publisherService.getOrderAmountTotal(date);

        //2.创建map集合用来存放新增日活数据
        HashMap<String, Object> dauMap = new HashMap<>();

        //3.创建map集合用来存放新增设备数据
        HashMap<String, Object> devMap = new HashMap<>();

        //创建存放交易额总数的map集合
        HashMap<String, Object> gmvMap = new HashMap<>();


        //4.将数据封装到Map集合中
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", amountTotal);


        //5.创建list集合用来存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //6.将封装好的map集合放入存放结果数据的list集合中
        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);

        //7.将list集合转为json字符串将其返回
        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String selectDauTotalHour(@RequestParam("id") String id, @RequestParam("date") String date) {

        //1.创建Map集合用来存放结果数据
        HashMap<String, Object> result = new HashMap<>();

        //2.获取service层处理完的数据
        //2.1获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        Map todayMap = null;
        Map yesterdayMap = null;

        if ("dau".equals(id)) {
            //2.2获取今天的数据
            todayMap = publisherService.getDauTotalHour(date);
            //2.3获取昨天的数据
            yesterdayMap = publisherService.getDauTotalHour(yesterday);

        } else if ("order_amount".equals(id)) {
            //获取今天交易额数据
            todayMap = publisherService.getOrderAmountHourMap(date);

            yesterdayMap = publisherService.getOrderAmountHourMap(yesterday);

        }
        //3.将数据封装到Map集合中
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        return JSONObject.toJSONString(result);
    }
}
