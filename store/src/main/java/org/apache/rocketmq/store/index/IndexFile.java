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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

/**
 * 只有存在key的消息才会写入索引信息
 * 提供了一种可以通过key或时间区间来查询消息的方法。
 *
 * ndex文件的存储位置是：$HOME \store\index${fileName}，文件名fileName是以创建时的时间戳命名的，
 * 固定的单个IndexFile文件大小约为400M，一个IndexFile可以保存 2000W个索引，
 * IndexFile的底层存储设计为在文件系统中实现HashMap结构，故rocketmq的索引文件其底层实现为hash索引。
 *
 * 组成： 1 IndexHeader + 500w个hash槽 + 2000Index条目
 *  IndexHeader = 40byte
 * Index 条目 = 4 hashcode  + 8 phyOffset + 4 timedif（该消息存储时间与第一条消息的时间戳差值） + 4  pre index no (上一条记录的index索引) = 20byte
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 槽的大小
    private static int hashSlotSize = 4;
    // Index条目的大小
    private static int indexSize = 20;

    private static int invalidIndex = 0;
    // 槽的数量，默认5,000,000
    private final int hashSlotNum;
    // Index条目的数量，默认20,000,000
    private final int indexNum;
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    // 头
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        // total = 40byte + 500w * 4 byte + 2000w * 20 byte = 420000040byte
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        // 这个为了处理hash slot及 index的
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;
        // 维护两套指针，这个为了处理Header的
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 写入索引
     * @param key 消息key
     * @param phyOffset 消息物理偏移量
     * @param storeTimestamp 消息存储时间
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            // 得到key的hash
            int keyHash = indexKeyHashMethod(key);
            // slotPos = keyHash % 500w
            int slotPos = keyHash % this.hashSlotNum;
            // 定位到slotPos在文件中的位置
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                // 获取absSlotPos对应的值
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // 如果之前不存在(新消息key)，用于下面是否新增slot数量
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                // 计算存储消息得时间戳与第一条消息得差值
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
                // 转换为秒
                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }
                // 定位到Index的位置
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;
                // Index->hash
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                // Index->phyOffset
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                // Index->timeDiff
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                // Index->slotValue
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
                // 之前的值，用于解决hash冲突
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());
                // 初始设置BeginPhyOffset和BeginTimestamp
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                if (invalidIndex == slotValue) {
                    this.indexHeader.incHashSlotCount();
                }
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }
    public void selectPhyOffset2(final List<Long> phyOffsets, final String key, final int maxNum,
                                final long begin, final long end, boolean lock) {
        if (!this.mappedFile.hold()) {
            return;
        }
        // key的hash值
        int keyHash = indexKeyHashMethod(key);
        // 得到hash槽
        int slotPos = keyHash % this.hashSlotNum;
        // 定位到hash槽的位置
        int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

        try {
            // 得到slot的值
            int slotValue = this.mappedByteBuffer.getInt(absSlotPos);

            if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                return;
            }
            for (int nextIndexToRead = slotValue; ; ) {
                // 找够了，结束
                if (phyOffsets.size() >= maxNum) {
                    break;
                }
                // 得到index的位置
                int absIndexPos =
                        IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;
                // key的hash值
                int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                // 物理偏移量
                long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                // 时间差值
                long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                // 上一个Index的位置
                int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                if (timeDiff < 0) {
                    break;
                }
                // 回复为毫秒
                timeDiff *= 1000L;
                // 获取存储时间
                long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                if (keyHash == keyHashRead && timeMatched) {
                    phyOffsets.add(phyOffsetRead);
                }

                if (prevIndexRead <= invalidIndex
                        || prevIndexRead > this.indexHeader.getIndexCount()
                        || prevIndexRead == nextIndexToRead || timeRead < begin) {
                    break;
                }
                // 找下一个消息
                nextIndexToRead = prevIndexRead;
            }

        } catch (Exception e) {
            log.error("selectPhyOffset exception ", e);
        } finally {
            this.mappedFile.release();
        }
    }
    /**
     *
     * @param phyOffsets 查找消息得物理偏移量
     * @param key 消息key
     * @param maxNum 本次查找最大消息条数
     * @param begin 开始时间戳
     * @param end 结束时间戳
     * @param lock
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            // key的hash值
            int keyHash = indexKeyHashMethod(key);
            // 得到hash槽
            int slotPos = keyHash % this.hashSlotNum;
            // 定位到hash槽的位置
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }
                // 得到slot的值
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                    //该索引文件还未生效
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        // 找够了，结束
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }
                        // 得到index的位置
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;
                        // key的hash值
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        // 物理偏移量
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                        // 时间差值
                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        // 上一个Index的位置
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }
                        // 回复为毫秒
                        timeDiff *= 1000L;
                        // 获取存储时间
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
