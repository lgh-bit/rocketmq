/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

/**
 * 消息消费队列，引入的目的主要是提高消息消费的性能
 *
 * Consumer即可根据ConsumeQueue来查找待消费的消息。其中，ConsumeQueue（逻辑消费队列）作为消费消息的索引，
 * 保存了指定Topic下的队列消息在CommitLog中的起始物理偏移量offset，消息大小size和消息Tag的HashCode值。
 * consumequeue文件可以看成是基于topic的commitlog索引文件。
 *
 * 存储路径: $HOME/store/consumequeue/{topic}/{queueId}/{fileName}
 *
 * consumequeue文件采取定长设计，每一个条目共20个字节，分别为8字节的commitlog物理偏移量、4字节的消息长度、8字节tag hashcode,
 * 单个文件由30W个条目组成，可以像数组一样随机访问每一个条目，每个ConsumeQueue文件大小约5.72M
 *
 *
 */
public class ConsumeQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 每一个条目共20个字节 len =  commitlog offset 8 +  size 4 + tag hashcode 8
    public static final int CQ_STORE_UNIT_SIZE = 20;
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    // 消息存储实现类
    private final DefaultMessageStore defaultMessageStore;
    // CommitLog的文件目录
    private final MappedFileQueue mappedFileQueue;
    // topic
    private final String topic;
    // queueId
    private final int queueId;
    /**
     * 索引文件的bytebuffer
     */
    private final ByteBuffer byteBufferIndex;
    // 存储路径
    private final String storePath;
    // 默认1G
    private final int mappedFileSize;
    // 最大物理偏移量
    private long maxPhysicOffset = -1;
    // 最小逻辑偏移量
    private volatile long minLogicOffset = 0;
    /**
     * 消费队列扩展
     */
    private ConsumeQueueExt consumeQueueExt = null;

    public ConsumeQueue(
        final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final DefaultMessageStore defaultMessageStore) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.defaultMessageStore = defaultMessageStore;

        this.topic = topic;
        this.queueId = queueId;

        String queueDir = this.storePath
            + File.separator + topic
            + File.separator + queueId;

        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

        //是否启用ConsumeQueue扩展属性
        if (defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            //启用就实例化ConsumeQueueExt
            this.consumeQueueExt = new ConsumeQueueExt(
                topic,
                queueId,
                StorePathConfigHelper.getStorePathConsumeQueueExt(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir()),
                defaultMessageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                defaultMessageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    /**
     * 加载
     */
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    /**
     * 用于设定maxPhysicOffset以及更新最后三个mappedFile的flush,commitPosition
     * 1.从前往后读取最后三个mappedFile，依次读取记录，如果有效，更新maxPhysicOffset以及maxExtAddr
     * 2.出现第一个无效记录的位置记为processOffset，设置flush,commitPosition，清除mappedFileQueue之后的记录
     * 3.如果开启ext，清除maxExtAddr之后的记录
     */
    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // 从倒数第三个文件开始恢复
            int index = mappedFiles.size() - 3;
            // 如果不足3个，从第一个开始
            if (index < 0)
                index = 0;

            int mappedFileSizeLogics = this.mappedFileSize;
            // 得到要恢复的文件
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            // 文件起始偏移量
            long processOffset = mappedFile.getFileFromOffset();
            //用来标记是否已经处理到末尾了
            long mappedFileOffset = 0;
            long maxExtAddr = 1;
            while (true) {
                // 以一个条目为跨度遍历，一次处理一条消息
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    // 得到偏移量
                    long offset = byteBuffer.getLong();
                    // 消息大小
                    int size = byteBuffer.getInt();
                    // tag
                    long tagsCode = byteBuffer.getLong();

                    if (offset >= 0 && size > 0) {
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        //更新最大物理偏移量
                        this.maxPhysicOffset = offset + size;
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                            + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }

                //处理最后一条消息。到达文件结尾，进行下一个文件
                if (mappedFileOffset == mappedFileSizeLogics) {
                    // 进行下一个ConsumeQueue的恢复
                    index++;
                    // 如果遍历完成，则结束
                    if (index >= mappedFiles.size()) {

                        log.info("recover last consume queue file over, last mapped file "
                            + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    log.info("recover current consume queue queue over " + mappedFile.getFileName() + " "
                        + (processOffset + mappedFileOffset));
                    break;
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            //删除后续记录
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }
    /**
     * 获取Offset在队列里根据时间
     * @param timestamp 时间戳
     */
    // 根据时间查找偏移量
    public long getOffsetInQueueByTime(final long timestamp) {
        // 根据时间戳找到对应的文件， //根据时间戳获取一个文件
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);
        if (mappedFile != null) {
            long offset = 0;
            // 计算最低查找偏移量=消息队列最小偏移量与文件起始偏移量的差值
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            int high = 0;
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long leftIndexValue = -1L, rightIndexValue = -1L;
            // 获取最小物理偏移量
            long minPhysicOffset = this.defaultMessageStore.getMinPhyOffset();
            //获取整个mappedFile
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                //选择出的文件中最后一条记录
                high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                try {
                    while (high >= low) {
                        //中位
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        byteBuffer.position(midOffset);
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();
                        //如果这个偏移量比文件最小的偏移量都小，就提高low
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            //左偏移量等于中位
                            leftOffset = midOffset;
                            continue;
                        }

                        long storeTime =
                            this.defaultMessageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            return 0;
                        } else if (storeTime == timestamp) {
                            targetOffset = midOffset;
                            break;
                        } else if (storeTime > timestamp) {
                            //更新high，往低移动一位
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            //右偏移量
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        } else {
                            //low往前移动一位
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            //左偏移量
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }

                    if (targetOffset != -1) {

                        offset = targetOffset;
                    } else {
                        if (leftIndexValue == -1) {

                            offset = rightOffset;
                        } else if (rightIndexValue == -1) {

                            offset = leftOffset;
                        } else {
                            offset =
                                Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp
                                    - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }

                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }
    public long getOffsetInQueueByTime2(final long timestamp) {
        // 根据时间戳找到对应的文件
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);
        if (Objects.isNull(mappedFile)) {
            return 0;
        }

        long offset = 0;
        // 计算最低查找偏移量=消息队列最小偏移量与文件起始偏移量的差值
        int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
        int high = 0;
        int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
        long leftIndexValue = -1L, rightIndexValue = -1L;
        // 获取最小物理偏移量
        long minPhysicOffset = this.defaultMessageStore.getMinPhyOffset();
        SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);
        if (Objects.isNull(sbr)) {
            return 0;
        }

        ByteBuffer byteBuffer = sbr.getByteBuffer();
        high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
        try {
            // 二分查找
            while (high >= low) {
                // 中间值
                midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                // 目的是为了获取中间值对应消息得偏移量及大小
                byteBuffer.position(midOffset);
                long phyOffset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                // 如果在右边，继续查找
                if (phyOffset < minPhysicOffset) {
                    low = midOffset + CQ_STORE_UNIT_SIZE;
                    leftOffset = midOffset;
                    continue;
                }
                // 找到中间消息的存储时间
                long storeTime =
                        this.defaultMessageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                if (storeTime < 0) {
                    // 无效消息，返回0
                    return 0;
                } else if (storeTime == timestamp) {
                    // 如果找到了，则结束二分查找
                    targetOffset = midOffset;
                    break;
                } else if (storeTime > timestamp) {
                    // 往左找
                    high = midOffset - CQ_STORE_UNIT_SIZE;
                    rightOffset = midOffset;
                    rightIndexValue = storeTime;
                } else {
                    // 往右找
                    low = midOffset + CQ_STORE_UNIT_SIZE;
                    leftOffset = midOffset;
                    leftIndexValue = storeTime;
                }
            }

            if (targetOffset != -1) {
                // 如果找到了记录offset
                offset = targetOffset;
            } else {
                // 如果没有找到

                if (leftIndexValue == -1) {
                    // 最右边
                    offset = rightOffset;
                } else if (rightIndexValue == -1) {
                    // 最左边
                    offset = leftOffset;
                } else {
                    offset =
                            Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp
                                    - rightIndexValue) ? rightOffset : leftOffset;
                }
            }

            return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
        } finally {
            sbr.release();
        }
    }

    /**
     * truncate脏文件通过物理偏移量
     */
    public void truncateDirtyLogicFiles(long phyOffet) {

        int logicFileSize = this.mappedFileSize;

        //更新最大的物理偏移量
        this.maxPhysicOffset = phyOffet;
        long maxExtAddr = 1;
        while (true) {
            //最后一个文件
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);

                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    if (0 == i) {
                        //如果offset大于传进来的offset,则删除最后一个文件
                        if (offset >= phyOffet) {
                            this.mappedFileQueue.deleteLastMappedFile();
                            break;
                        } else {
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset + size;
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }
                    } else {

                        if (offset >= 0 && size > 0) {

                            if (offset >= phyOffet) {
                                return;
                            }

                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset + size;
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }

                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }
            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    /**
     * 获取最后一个偏移量
     */
    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {

            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }


    /**
     * 刷盘
     * @param flushLeastPages 刷盘页数
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    /**
     * 删除过期的文件
     * @param offset offset
     */
    public int deleteExpiredFile(long offset) {
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        this.correctMinOffset(offset);
        return cnt;
    }

    /**
     * 纠正的最小偏移量
     * @param phyMinOffset ;
     */
    public void correctMinOffset(long phyMinOffset) {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(0);
            if (result != null) {
                try {
                    for (int i = 0; i < result.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        long offsetPy = result.getByteBuffer().getLong();
                        result.getByteBuffer().getInt();
                        long tagsCode = result.getByteBuffer().getLong();

                        if (offsetPy >= phyMinOffset) {
                            this.minLogicOffset = mappedFile.getFileFromOffset() + i;
                            log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                                this.getMinOffsetInQueue(), this.topic, this.queueId);
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                minExtAddr = tagsCode;
                            }
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception thrown when correctMinOffset", e);
                } finally {
                    result.release();
                }
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    /**
     * 获取队列最小的offset
     */
    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    /**
     * 添加逻辑队列的信息
     * @param request 请求
     */
    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        final int maxRetries = 30;
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            long tagsCode = request.getTagsCode();
            if (isExtWriteEnable()) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());

                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                        topic, queueId, request.getCommitLogOffset());
                }
            }
            // 核心
            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(),
                request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());
            if (result) {
                if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE ||
                    this.defaultMessageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
                    this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(request.getStoreTimestamp());
                }
                this.defaultMessageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                    + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
    }

    /**
     * 添加消息到逻辑队列
     * @param offset commitlog物理偏移量
     * @param size size
     * @param tagsCode tags的哈希code
     * @param cqOffset 消费队列偏移量
     * @return ;
     */
    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,
        final long cqOffset) {

        if (offset + size <= this.maxPhysicOffset) {
            log.warn("Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}", maxPhysicOffset, offset);
            return true;
        }
        // flip
        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        // 消息偏移量
        this.byteBufferIndex.putLong(offset);
        // 消息长度
        this.byteBufferIndex.putInt(size);
        // tag hashcode
        this.byteBufferIndex.putLong(tagsCode);

        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;

        // 获取对应的ConsumeQueue文件
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {
            // 第一次写入时追加空白？
            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                this.minLogicOffset = expectLogicOffset;
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                    + mappedFile.getWrotePosition());
            }

            if (cqOffset != 0) {
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }

                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn(
                        "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset,
                        currentLogicOffset,
                        this.topic,
                        this.queueId,
                        expectLogicOffset - currentLogicOffset
                    );
                }
            }
            this.maxPhysicOffset = offset + size;
            // 追加消息
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }
        return false;
    }

    /**
     * 填充空值
     */
    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }
    // 根据startIndex获取消息消费队列条目
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        // 得到在ConsumeQueue中的偏移量
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        // 是否存在
        if (offset >= this.getMinLogicOffset()) {
            // 通过偏移量找到对应的MappedFile
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
                // offset % mappedFileSize找到在文件中偏移量(逻辑偏移量)
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
                return result;
            }
        }
        return null;
    }

    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    public long rollNextFile(final long index) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        return index + totalUnitsInFile - index % totalUnitsInFile;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    /**
     * 是否可以读extRead
     */
    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    /**
     * 是否可以写
     */
    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
            && this.defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }
}
