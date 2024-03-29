package com.alibaba.lindorm.contest.impl.wtire;

import com.alibaba.lindorm.contest.impl.compress.DeflaterUtils;
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
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FlushRequestTask extends Thread {
    private final FileManager fileManager;
    private boolean stop = false;
    private final HandleRequestTask handleRequestTask;
    private final ByteBuffersDataOutput byteBuffersDataOutput;

    public FlushRequestTask(FileManager fileManager, HandleRequestTask handleRequestTask) {
        this.handleRequestTask = handleRequestTask;
        this.fileManager = fileManager;
        byteBuffersDataOutput = new ByteBuffersDataOutput();
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
            Collection<Row> rows = writeRequest.getRows();
            for (Row row : rows) {
                Vin vin = row.getVin();

                FileChannel dataWriteFileChanel = fileManager.getWriteFilChannel(tableName, vin);
                SchemaMeta schemaMeta = fileManager.getSchemaMeta(tableName);

                ArrayList<String> integerColumnsNameList = schemaMeta.getIntegerColumnsName();
                for (String cName : integerColumnsNameList) {
                    ColumnValue cVal = row.getColumns().get(cName);
                    ColumnValue.IntegerColumn integerColumn = (ColumnValue.IntegerColumn) cVal;
                    byteBuffersDataOutput.writeVInt(integerColumn.getIntegerValue());
                }

                ArrayList<String> doubleColumnsNameList = schemaMeta.getDoubleColumnsName();
                for (String cName : doubleColumnsNameList) {
                    ColumnValue cVal = row.getColumns().get(cName);
                    ColumnValue.DoubleFloatColumn doubleFloatColumn = (ColumnValue.DoubleFloatColumn) cVal;
                    byteBuffersDataOutput.writeZDouble(doubleFloatColumn.getDoubleFloatValue());
                }

                ArrayList<String> stringColumnsNameList = schemaMeta.getStringColumnsName();
                for (String cName : stringColumnsNameList) {
                    ColumnValue cVal = row.getColumns().get(cName);
                    ColumnValue.StringColumn stringColumn = (ColumnValue.StringColumn) cVal;
                    ByteBuffer stringValue = stringColumn.getStringValue();
                    byteBuffersDataOutput.writeString(new String(stringValue.array()));
                }

                //获得写文件锁
                FileLock lock = dataWriteFileChanel.lock();

                long position = dataWriteFileChanel.position();
                Index index = new Index();
                index.setOffset(position);
                index.setRowKey(vin.getVin());
                index.setLatestTimestamp(row.getTimestamp());

                //压缩
                ByteBuffer totalByte = ByteBuffer.allocate((int) byteBuffersDataOutput.size());
                for (int i = 0; i < byteBuffersDataOutput.toWriteableBufferList().size(); i++) {
                    totalByte.put(byteBuffersDataOutput.toWriteableBufferList().get(i));
                }
                totalByte.flip();
                byte[] zipBytes = DeflaterUtils.zipString(totalByte.array());
                ByteBuffersDataOutput tempOutput = new ByteBuffersDataOutput();
                tempOutput.writeVLong(row.getTimestamp());
                tempOutput.writeVInt(zipBytes.length);
                tempOutput.writeBytes(zipBytes);
                totalByte = ByteBuffer.allocate((int) tempOutput.size());
                for (ByteBuffer byteBuffer : tempOutput.toBufferList()) {
                    totalByte.put(byteBuffer);
                }
                totalByte.flip();
                dataWriteFileChanel.write(totalByte);

                //add index
                index.setBuffer(ByteBuffer.wrap(zipBytes));
                IndexLoader.offerLatestIndex(tableName, vin, index);

                //释放写文件锁
                lock.close();

                byteBuffersDataOutput.reset();
            }
        }

        iterator = writeRequestWrapperList.iterator();
        while (iterator.hasNext()) {
            WriteRequestWrapper writeRequestWrapper = iterator.next();
            //释放锁让写线程返回
            writeRequestWrapper.getLock().lock();
            writeRequestWrapper.getCondition().signal();
            writeRequestWrapper.getLock().unlock();
        }
    }
}
