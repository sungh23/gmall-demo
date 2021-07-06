package com.admin.dwpublisher.service.impl;

import com.admin.dwpublisher.mapper.DauMapper;
import com.admin.dwpublisher.mapper.OrderMapper;
import com.admin.dwpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author sungaohua
 */
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHour(String date) {
        //1.获取数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //1.2创建map集合用来存放新的数据
        HashMap<String, Long> hourMap = new HashMap<>();

        //2.遍历list集合
        for (Map map : list) {
            hourMap.put((String) map.get("LH"), (Long) map.get("CT"));
        }
        return hourMap;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, Double> getOrderAmountHourMap(String date) {
        //获取数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //创建新的map用于结果数据
        HashMap<String, Double> result = new HashMap<>();

        //遍历list集合将老Map的数据转换结构存入新map
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }

        return result;

    }
}