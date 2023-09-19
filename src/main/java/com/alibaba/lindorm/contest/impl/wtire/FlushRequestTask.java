package com.alibaba.lindorm.contest.impl.wtire;

import com.alibaba.lindorm.contest.impl.common.CommonSetting;
import com.alibaba.lindorm.contest.impl.file.FileManager;
import com.alibaba.lindorm.contest.impl.index.Index;
import com.alibaba.lindorm.contest.impl.index.IndexLoader;
import com.alibaba.lindorm.contest.impl.schema.SchemaMeta;
import com.alibaba.lindorm.contest.impl.store.ByteBuffersDataOutput;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.structs.WriteRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FlushRequestTask extends Thread {
    private final FileManager fileManager;
    private boolean stop = false;
    private final HandleRequestTask handleRequestTask;
    private final ByteBuffersDataOutput latestDataOutput;

    public FlushRequestTask(FileManager fileManager, HandleRequestTask handleRequestTask) {
        this.handleRequestTask = handleRequestTask;
        this.fileManager = fileManager;
        latestDataOutput = new ByteBuffersDataOutput();
    }

    public void shutdown() {
        stop = true;
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                List<WriteRequestWrapper> writeRequestWrapperList = handleRequestTask.getFlushRequestQueue().poll(5, TimeUnit.MILLISECONDS);
                if (writeRequestWrapperList != null && writeRequestWrapperList.size() > 0) {
                    //执行刷盘操作
                    doWrite(writeRequestWrapperList);
                }
            } catch (Exception e) {
                System.out.println(">>> " + Thread.currentThread().getName() + " FlushRequestTask happen exception: " + e.getMessage());
                System.out.println(">>> " + Thread.currentThread().getName() + " FlushRequestTask happen exception: " + e.getClass().getName());
                for (StackTraceElement stackTraceElement : e.getStackTrace()) {
                    System.out.println(">>> " + Thread.currentThread().getName() + " FlushRequestTask happen exception: " + stackTraceElement.toString());
                }
                System.exit(-1);
            }
        }
    }

    private void doWrite(List<WriteRequestWrapper> writeRequestWrapperList) throws IOException {
        //保存KV数据
        Iterator<WriteRequestWrapper> iterator = writeRequestWrapperList.iterator();
        while (iterator.hasNext()) {
            WriteRequestWrapper writeRequestWrapper = iterator.next();

            WriteRequest writeRequest = writeRequestWrapper.getWriteRequest();
            String tableName = writeRequest.getTableName();

            SchemaMeta schemaMeta = fileManager.getSchemaMeta(tableName);
            Map<Vin, Map<String, ByteBuffersDataOutput>> vinByteBuffersDataOutputMap = new HashMap<>();
            Collection<Row> rows = writeRequest.getRows();
            for (Row row : rows) {
                long delta = row.getTimestamp() - CommonSetting.DEFAULT_TIMESTAMP;
                Vin vin = row.getVin();
                Map<String, ByteBuffersDataOutput> byteBuffersDataOutputMap = vinByteBuffersDataOutputMap.get(vin);
                if (byteBuffersDataOutputMap == null) {
                    byteBuffersDataOutputMap = new HashMap<>();
                    ArrayList<String> columnsNameList = new ArrayList<>();
                    columnsNameList.addAll(schemaMeta.getIntegerColumnsName());
                    columnsNameList.addAll(schemaMeta.getDoubleColumnsName());
                    columnsNameList.addAll(schemaMeta.getStringColumnsName());
                    for (String cName : columnsNameList) {
                        byteBuffersDataOutputMap.put(cName, new ByteBuffersDataOutput());
                    }
                    vinByteBuffersDataOutputMap.put(vin, byteBuffersDataOutputMap);
                }

                ArrayList<String> integerColumnsNameList = schemaMeta.getIntegerColumnsName();
                for (String cName : integerColumnsNameList) {
                    ColumnValue cVal = row.getColumns().get(cName);
                    ColumnValue.IntegerColumn integerColumn = (ColumnValue.IntegerColumn) cVal;
                    int integerValue = integerColumn.getIntegerValue();
                    latestDataOutput.writeVInt(integerValue);
                    ByteBuffersDataOutput byteBuffersDataOutput = byteBuffersDataOutputMap.get(cName);
                    byteBuffersDataOutput.writeVLong(delta);
                    byteBuffersDataOutput.writeVInt(integerValue);
                }

                ArrayList<String> doubleColumnsNameList = schemaMeta.getDoubleColumnsName();
                for (String cName : doubleColumnsNameList) {
                    ColumnValue cVal = row.getColumns().get(cName);
                    ColumnValue.DoubleFloatColumn doubleFloatColumn = (ColumnValue.DoubleFloatColumn) cVal;
                    double doubleFloatValue = doubleFloatColumn.getDoubleFloatValue();
                    latestDataOutput.writeZDouble(doubleFloatValue);
                    ByteBuffersDataOutput byteBuffersDataOutput = byteBuffersDataOutputMap.get(cName);
                    byteBuffersDataOutput.writeVLong(delta);
                    byteBuffersDataOutput.writeZDouble(doubleFloatValue);
                }

                ArrayList<String> stringColumnsNameList = schemaMeta.getStringColumnsName();
                for (String cName : stringColumnsNameList) {
                    ColumnValue cVal = row.getColumns().get(cName);
                    ColumnValue.StringColumn stringColumn = (ColumnValue.StringColumn) cVal;
                    ByteBuffer stringValue = stringColumn.getStringValue();
                    String value = new String(stringValue.array());
                    latestDataOutput.writeString(value);
                    ByteBuffersDataOutput byteBuffersDataOutput = byteBuffersDataOutputMap.get(cName);
                    byteBuffersDataOutput.writeVLong(delta);
                    byteBuffersDataOutput.writeString(value);
                }

                Index index = new Index();
                index.setRowKey(vin.getVin());
                index.setDelta(delta);

                ByteBuffer totalByte = ByteBuffer.allocate((int) latestDataOutput.size());
                for (int i = 0; i < latestDataOutput.toWriteableBufferList().size(); i++) {
                    totalByte.put(latestDataOutput.toWriteableBufferList().get(i));
                }
                totalByte.flip();
                byte[] bytes = totalByte.array();

                //add index
                index.setBytes(bytes);
                index.setBuffer(ByteBuffer.wrap(bytes));
                IndexLoader.offerLatestIndex(tableName, vin, index);

                latestDataOutput.reset();
            }

            for (Map.Entry<Vin, Map<String, ByteBuffersDataOutput>> entry : vinByteBuffersDataOutputMap.entrySet()) {
                for (Map.Entry<String, ByteBuffersDataOutput> e : entry.getValue().entrySet()) {
                    FileChannel dataWriteFileChanel = fileManager.getWriteFilChannel(tableName, entry.getKey(), e.getKey());
                    ByteBuffer totalByte = ByteBuffer.allocate((int) e.getValue().size());
                    for (int i = 0; i < e.getValue().toWriteableBufferList().size(); i++) {
                        totalByte.put(e.getValue().toWriteableBufferList().get(i));
                    }
                    totalByte.flip();
                    dataWriteFileChanel.write(totalByte);
                }
            }

            //释放锁让写线程返回
            writeRequestWrapper.getLock().lock();
            writeRequestWrapper.getCondition().signal();
            writeRequestWrapper.getLock().unlock();
        }
    }
}
