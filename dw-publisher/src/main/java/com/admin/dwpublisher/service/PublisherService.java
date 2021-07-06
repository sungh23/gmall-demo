package com.admin.dwpublisher.service;

import java.util.Map;

/**
 * @author sungaohua
 */
public interface PublisherService {
    Integer getDauTotal(String date);

    Map getDauTotalHour(String date);

    //交易额总数
    Double getOrderAmountTotal(String date);

    //交易额分时数据
    Map<String, Double> getOrderAmountHourMap(String date);

}
