package com.admin.dwpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author sungaohua
 */
public interface DauMapper {
    Integer selectDauTotal(String date);

    List<Map> selectDauTotalHourMap(String date);
}
