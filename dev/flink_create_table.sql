CREATE TABLE dwd_hbase_orders (
  rowkey string,
  cf ROW<createTime VARCHAR, goodId VARCHAR, goodsMoney VARCHAR, orderId VARCHAR, orderNo VARCHAR, payFrom VARCHAR, province VARCHAR, realTotalMoney VARCHAR, userId VARCHAR >
) WITH (
  'connector.type' = 'hbase',
  'connector.version' = '1.4.3',
  'connector.table-name' = 'dwd:orders',
  'connector.zookeeper.quorum' = 'crxy103:2181,crxy104:2181,crxy105:2181',
  'connector.zookeeper.znode.parent' = '/hbase',
  'connector.write.buffer-flush.max-size' = '10mb',
  'connector.write.buffer-flush.max-rows' = '1000',
  'connector.write.buffer-flush.interval' = '2s'
);

select * from dwd_hbase_orders;


SELECT MD5(USERID)||'_'||USERID as rowkey,ROW(USERID,ORDER_NUM) AS CF FROM ( SELECT USERID,count(distinct ORDERID) as ORDER_NUM  FROM flink_dwd_orders group by USERID ) TEMP

