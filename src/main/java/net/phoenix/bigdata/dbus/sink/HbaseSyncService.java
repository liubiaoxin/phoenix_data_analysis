package net.phoenix.bigdata.dbus.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.protocol.FlatMessage;
import lombok.extern.slf4j.Slf4j;
import net.phoenix.bigdata.common.enums.HBaseStorageModeEnum;
import net.phoenix.bigdata.common.utils.*;
import net.phoenix.bigdata.pojo.Flow;
import net.phoenix.bigdata.pojo.PhType;
import net.phoenix.bigdata.pojo.Type;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.*;

/**
 * HBase同步操作业务
 */
@Slf4j
public class HbaseSyncService implements Serializable {
    private HbaseTemplate hbaseTemplate;                                    // HBase操作模板

    public HbaseSyncService(HbaseTemplate hbaseTemplate){
        this.hbaseTemplate = hbaseTemplate;
    }

    public void sync(Flow flow, FlatMessage dml) {
        if (flow != null) {
            String type = dml.getType();
            if (type != null && type.equalsIgnoreCase("INSERT")) {
                insert(flow, dml);
            } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                update(flow, dml);
            } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                delete(flow, dml);
            }
            if (log.isDebugEnabled()) {
                log.debug("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
            }
        }
    }

    /**
     * 插入操作
     *
     * @param flow 配置项
     * @param dml DML数据
     */
    private void insert(Flow flow, FlatMessage dml) {
        List<Map<String, String>> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        int i = 1;
        boolean complete = false;
        List<HRow> rows = new ArrayList<>();
        for (Map<String, String> r : data) {
            HRow hRow = new HRow();

            // 拼接复合rowKey

            if (flow.getRowKey() != null) {
                String[] rowKeyColumns = flow.getRowKey().trim().split(",");
                String rowKeyVale = getRowKey(rowKeyColumns, r);
                hRow.setRowKey(Bytes.toBytes(rowKeyVale));
            }

            convertData2Row(flow, hRow, r);
            if (hRow.getRowKey() == null) {
                throw new RuntimeException("empty rowKey: " + hRow.toString()+",Flow: "+flow.toString());
            }
            rows.add(hRow);
            complete = false;

            if (i % flow.getCommitBatch() == 0 && !rows.isEmpty()) {
                hbaseTemplate.puts(flow.getHbaseTable(), rows);
                rows.clear();
                complete = true;
            }
            i++;
        }
        if (!complete && !rows.isEmpty()) {
            hbaseTemplate.puts(flow.getHbaseTable(), rows);
        }

    }

    /**
     * 更新操作
     *
     * @param flow 配置对象
     * @param dml dml对象
     */
    private void update(Flow flow, FlatMessage dml) {

        //修改后的数据
        List<Map<String, String>> data = dml.getData();
        //修改之前的数据
        List<Map<String, String>> old = dml.getOld();

        //数据为空，直接返回
        if(old==null || old.isEmpty() ||data==null || data.isEmpty()){
            return;
        }

        String rowKeyColumn = flow.getRowKey();
        int index = 0;
        int i = 1;
        boolean complete = false;
        List<HRow> rows = new ArrayList<>();
        out: for (Map<String, String> r : data) {
            byte[] rowKeyBytes;

            if (flow.getRowKey() != null) {
                String[] rowKeyColumns = flow.getRowKey().trim().split(",");

                // 判断是否有复合主键修改,如果复合主键修改，则直接删除，重新插入
                for (String updateColumn : old.get(index).keySet()) {
                    for (String rowKeyColumnName : rowKeyColumns) {
                        if (rowKeyColumnName.equalsIgnoreCase(updateColumn)) {
                            // 调用删除插入操作
                            deleteAndInsert(flow, dml);
                            continue out;
                        }
                    }
                }

                String rowKeyVale = getRowKey(rowKeyColumns, r);
                rowKeyBytes = Bytes.toBytes(rowKeyVale);
            } else if (rowKeyColumn == null) {
                rowKeyBytes = typeConvert(flow, dml, r.values().iterator().next());
            } else {
                rowKeyBytes = getRowKeyBytes(flow,dml, rowKeyColumn, r);
            }
            if (rowKeyBytes == null) throw new RuntimeException("rowKey值为空");

            HRow hRow = new HRow(rowKeyBytes);
            for (String updateColumn : old.get(index).keySet()) {
                if (rowKeyColumn.contains(updateColumn)) {
                    continue;
                }
                if (r == null) {
                    String family = flow.getFamily();
                    String qualifier = updateColumn;
                    if (flow.isUppercaseQualifier()) {
                        qualifier = qualifier.toUpperCase();
                    }

                    Object newVal = r.get(updateColumn);

                    if (newVal == null) {
                        hRow.addCell(family, qualifier, null);
                    } else {
                        hRow.addCell(family, qualifier, typeConvert(flow, dml, newVal));
                    }
                } else {
                    // 排除修改id的情况
                    if (rowKeyColumn.contains(updateColumn)) continue;

                    Object newVal = r.get(updateColumn);
                    String qualifier = updateColumn;
                    if (flow.isUppercaseQualifier()) {
                        qualifier = qualifier.toUpperCase();
                    }
                    if (newVal == null) {
                        hRow.addCell(flow.getFamily(),qualifier, null);
                    } else {
                        hRow.addCell(flow.getFamily(), qualifier, typeConvert(flow, dml, newVal));
                    }
                }
            }
            rows.add(hRow);

            complete = false;
            if (i % flow.getCommitBatch() == 0 && !rows.isEmpty()) {
                hbaseTemplate.puts(flow.getHbaseTable(), rows);
                rows.clear();
                complete = true;
            }
            i++;
            index++;
        }
        if (!complete && !rows.isEmpty()) {
            hbaseTemplate.puts(flow.getHbaseTable(), rows);
        }



    }

    /* **
     * 删除操作
     **/
    private void delete(Flow flow, FlatMessage dml) {
        List<Map<String, String>> data = dml.getData();
        if(data==null && data.isEmpty()){
            return;
        }
        boolean complete = false;
        int i = 1;
        Set<byte[]> rowKeys = new HashSet<>();
        for (Map<String, String> r : data) {
            byte[] rowKeyBytes;

            String rowKeyColumn = flow.getRowKey();

            if (rowKeyColumn != null) {
                String[] rowKeyColumns = rowKeyColumn.trim().split(",");
                String rowKeyVale = getRowKey(rowKeyColumns, r);
                rowKeyBytes = Bytes.toBytes(rowKeyVale);
            } else if (rowKeyColumn == null) {
                // 如果不需要类型转换
                rowKeyBytes = typeConvert(flow, dml, r.values().iterator().next());
            } else {
                rowKeyBytes = getRowKeyBytes(flow,dml, rowKeyColumn, r);
            }
            if (rowKeyBytes == null) throw new RuntimeException("rowKey值为空");
            rowKeys.add(rowKeyBytes);
            complete = false;
            if (i % flow.getCommitBatch() == 0 && !rowKeys.isEmpty()) {
                hbaseTemplate.deletes(flow.getHbaseTable(), rowKeys);
                rowKeys.clear();
                complete = true;
            }
            i++;
        }
        if (!complete && !rowKeys.isEmpty()) {
            hbaseTemplate.deletes(flow.getHbaseTable(), rowKeys);
        }

    }

    /* **
     * 删除插入操作
     **/
    private void deleteAndInsert(Flow flow, FlatMessage dml) {
        List<Map<String, String>> data = dml.getData();
        List<Map<String, String>> old = dml.getOld();
        if (old == null || old.isEmpty() || data == null || data.isEmpty()) {
            return;
        }
        String[] rowKeyColumns = flow.getRowKey().trim().split(",");

        int index = 0;
        int i = 1;
        boolean complete = false;
        Set<byte[]> rowKeys = new HashSet<>();
        List<HRow> rows = new ArrayList<>();
        for (Map<String, String> r : data) {
            // 拼接老的rowKey
            List<String> updateSubRowKey = new ArrayList<>();
            for (String rowKeyColumnName : rowKeyColumns) {
                for (String updateColumn : old.get(index).keySet()) {
                    if (rowKeyColumnName.equalsIgnoreCase(updateColumn)) {
                        updateSubRowKey.add(rowKeyColumnName);
                    }
                }
            }
            if (updateSubRowKey.isEmpty()) {
                throw new RuntimeException("没有更新复合主键的RowKey");
            }
            StringBuilder oldRowKey = new StringBuilder();
            StringBuilder newRowKey = new StringBuilder();
            for (String rowKeyColumnName : rowKeyColumns) {
                newRowKey.append(r.get(rowKeyColumnName).toString()).append("|");
                if (!updateSubRowKey.contains(rowKeyColumnName)) {
                    // 从data取
                    oldRowKey.append(r.get(rowKeyColumnName).toString()).append("|");
                } else {
                    // 从old取
                    oldRowKey.append(old.get(index).get(rowKeyColumnName).toString()).append("|");
                }
            }
            int len = newRowKey.length();
            newRowKey.delete(len - 1, len);
            len = oldRowKey.length();
            oldRowKey.delete(len - 1, len);

            //自定义rowkey规则：MD5组合key前8位+key1|key2
            byte[] newRowKeyBytes = Bytes.toBytes(Md5Utils.getMD5String(newRowKey.toString()).substring(0, 8) + "_" +newRowKey.toString());
            byte[] oldRowKeyBytes = Bytes.toBytes(Md5Utils.getMD5String(oldRowKey.toString()).substring(0, 8) + "_" +oldRowKey.toString());

            rowKeys.add(oldRowKeyBytes);
            HRow row = new HRow(newRowKeyBytes);
            convertData2Row(flow, row, r);
            rows.add(row);
            complete = false;
            if (i % flow.getCommitBatch() == 0 && !rows.isEmpty()) {
                hbaseTemplate.deletes(flow.getHbaseTable(), rowKeys);

                hbaseTemplate.puts(flow.getHbaseTable(), rows);
                rowKeys.clear();
                rows.clear();
                complete = true;
            }
            i++;
            index++;
        }
        if (!complete && !rows.isEmpty()) {
            hbaseTemplate.deletes(flow.getHbaseTable(), rowKeys);
            hbaseTemplate.puts(flow.getHbaseTable(), rows);
        }
    }




    /**
     * 获取复合字段作为rowKey的拼接
     *
     * @param rowKeyColumns 复合rowK对应的字段
     * @param data 数据
     * @return
     */
    private static String getRowKey(String[] rowKeyColumns, Map<String, String> data) {
        StringBuilder rowKeyValue = new StringBuilder();
        for (String rowKeyColumnName : rowKeyColumns) {
            Object obj = data.get(rowKeyColumnName);
            if (obj != null) {
                rowKeyValue.append(obj.toString());
            }
            rowKeyValue.append("|");
        }
        int len = rowKeyValue.length();
        if (len > 0) {
            rowKeyValue.delete(len - 1, len);
        }

        //可自行扩展支持多种rowkey生成策略，这里写死为md5前缀
        return Md5Utils.getMD5String(rowKeyValue.toString()).substring(0, 8) + "_" + rowKeyValue.toString();
    }


    /**
     * 将Map数据转换为HRow行数据
     *
     * @param flow hbase映射配置
     * @param hRow 行对象
     * @param data Map数据
     */
    private static void convertData2Row(Flow flow, HRow hRow, Map<String, String> data) {
        String familyName = flow.getFamily();

        for (Map.Entry<String, String> entry : data.entrySet()) {
            if (entry.getValue() != null) {

                byte[] bytes = Bytes.toBytes(entry.getValue().toString());

                String qualifier = entry.getKey();
                if (flow.isUppercaseQualifier()) {
                    qualifier = qualifier.toUpperCase();
                }

                hRow.addCell(familyName, qualifier, bytes);
            }
        }
    }

    private static byte[] getRowKeyBytes(Flow flow,FlatMessage dml, String rowKeyColumn,
                                         Map<String, String> rowData) {
        Object val = rowData.get(rowKeyColumn);
        String v = null;
        if (rowKeyColumn.length()>0) {
            if (val instanceof Number) {
                v = String.format("%0" + rowKeyColumn.length() + "d", (Number) ((Number) val).longValue());
            } else if (val instanceof String) {
                v = String.format("%0" + rowKeyColumn.length() + "d", Long.parseLong((String) val));
            }
        }
        if (v != null) {
            return Bytes.toBytes(v);
        } else {
            return typeConvert(flow, dml, val);
        }
    }

    /**
     * 根据对应的类型进行转换
     *
     * @param value 值
     * @return 复合字段rowKey
     */
    private static byte[] typeConvert(Flow flow,FlatMessage dml,
                                      Object value) {
        if (value == null) {
            return null;
        }
        byte[] bytes = null;
        if (dml == null || dml.getType() == null || "".equals(dml.getType())) {
            if (HBaseStorageModeEnum.STRING.getCode()== flow.getMode()) {
                bytes = Bytes.toBytes(value.toString());
            } else if (HBaseStorageModeEnum.NATIVE.getCode() == flow.getMode()) {
                bytes = TypeUtil.toBytes(value);
            } else if (HBaseStorageModeEnum.PHOENIX.getCode() == flow.getMode()) {
                PhType phType = PhType.getType(value.getClass());
                bytes = PhTypeUtil.toBytes(value, phType);
            }
        } else {
            if (flow.getMode() == HBaseStorageModeEnum.STRING.getCode()) {
                bytes = Bytes.toBytes(value.toString());
            } else if (flow.getMode() == HBaseStorageModeEnum.NATIVE.getCode()) {
                //Type type = Type.getType(dml.getType());
                Type type = Type.getType(value.getClass());
                bytes = TypeUtil.toBytes(value, type);
            } else if (flow.getMode() == HBaseStorageModeEnum.PHOENIX.getCode()) {
                //PhType phType = PhType.getType(dml.getType());
                PhType phType = PhType.getType(value.getClass());
                bytes = PhTypeUtil.toBytes(value, phType);
            }
        }
        return bytes;
    }
}
