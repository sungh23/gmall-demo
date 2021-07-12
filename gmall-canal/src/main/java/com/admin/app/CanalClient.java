package com.admin.app;

import com.admin.constants.GmallConstants;
import com.admin.utils.MyKafkaSender;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author sungaohua
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        // 1 获取canal连接器
        CanalConnector canal = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        //2、为了一直监控Mysql数据库 ，所以要写成一个死循环
        while (true) {
            // 3 通过连接器获取连接
            canal.connect();

            // 4 订阅要监控的数据库
            canal.subscribe("gmall.*");

            // 5 获取多个sql执行结果
            Message message = canal.get(100);
            // 6 获取每个sql执行结果的entry
            List<CanalEntry.Entry> entries = message.getEntries();
            // 7 判断是否有数据
            if (entries.size() > 0) {
                for (CanalEntry.Entry entry : entries) {
                    // 8 todo 获取表名
                    String tableName = entry.getHeader().getTableName();
                    // 9 获取entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    // RowChange类型才有数据
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        // 10 获取序列化数据
                        ByteString storeValue = entry.getStoreValue();

                        // 11反序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        // 12 获取每一行的数据集（一个sql对应的实际数据）
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        // 13 获取事件类型  （insert update del，，，）
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        // 14 处理数据
                        handle(tableName, rowDatasList, eventType);


                    }

                }

            } else {
                System.out.println("没有数据，休息5秒钟");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }

    }

    /**
     * 解析具体的数据
     *
     * @param tableName
     * @param rowDatasList
     * @param eventType
     */
    private static void handle(String tableName, List<CanalEntry.RowData> rowDatasList, CanalEntry.EventType eventType) {
        //筛选数据 (对order_info 表插入数据操作 )  订单和订单详情表只需要关注新增数据即可
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendKafkaData(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER);

        }else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
            // 获取每一条数据
            sendKafkaData(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);

            // 用户表需要关注新增和变化的内容
        }else if ("user_info".equals(tableName) &&(CanalEntry.EventType.INSERT.equals(eventType)||CanalEntry.EventType.UPDATE.equals(eventType))){
            sendKafkaData(rowDatasList,GmallConstants.KAFKA_TOPIC_USER);
        }

    }

    /**
     *  发送数据到kafka队列
     * @param rowDatasList
     * @param kafkaTopicOrder
     */
    private static void sendKafkaData(List<CanalEntry.RowData> rowDatasList, String kafkaTopicOrder) {
        // 获取每一条数据
        for (CanalEntry.RowData rowData : rowDatasList) {
            // 获取修改之后的数据
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            // 获取每一条数据
            JSONObject json = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                json.put(column.getName(), column.getValue());
            }
            System.out.println(json.toJSONString());
            MyKafkaSender.send(kafkaTopicOrder, json.toJSONString());
        }
    }
}
