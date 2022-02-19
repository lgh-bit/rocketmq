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

package org.apache.rocketmq.client.latency;

import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
/**
 * broker队列选择的容错策略选择器
 */
public class LatencyFaultToleranceImpl implements LatencyFaultTolerance<String> {
    // 记录不可用的Broker
    /**
     * key：brokerName,value：broker下次可用的时间，与延迟的相关记录
     */
    private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<String, FaultItem>(16);
    // 取可用列表中前面一半的指针
    private final ThreadLocalIndex whichItemWorst = new ThreadLocalIndex();

    /**
     * 更新
     * @param name brokerName
     * @param currentLatency 当前延迟时间
     * @param notAvailableDuration 下次可用时间
     */
    @Override
    public void updateFaultItem(final String name, final long currentLatency, final long notAvailableDuration) {
        // 看是否存在
        FaultItem old = this.faultItemTable.get(name);
        if (null == old) {
            // 如果不存在则创建FaultItem
            final FaultItem faultItem = new FaultItem(name);
            faultItem.setCurrentLatency(currentLatency);
            // 设置再次可用时间
            //设置startTimesStamp为下次可用的时间。当前时间+不可用周期
            faultItem.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);

            old = this.faultItemTable.putIfAbsent(name, faultItem);
            if (old != null) {
                old.setCurrentLatency(currentLatency);
                old.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
            }
        } else {
            old.setCurrentLatency(currentLatency);
            // 设置再次可用时间
            old.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
        }
    }

    /**
     * 某个broker是否可用
     */
    @Override
    public boolean isAvailable(final String name) {
        final FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem != null) {
            // 判断是否到时间了
            return faultItem.isAvailable();
        }
        // 如果不在不可用表里，那就是可用的
        return true;
    }

    @Override
    public void remove(final String name) {
        this.faultItemTable.remove(name);
    }

    /**
     * 找到一个至少可用的BrokerName
     */
    @Override
    public String pickOneAtLeast() {
        final Enumeration<FaultItem> elements = this.faultItemTable.elements();
        List<FaultItem> tmpList = new LinkedList<FaultItem>();
        while (elements.hasMoreElements()) {
            final FaultItem faultItem = elements.nextElement();
            tmpList.add(faultItem);
        }

        if (!tmpList.isEmpty()) {
            // 为了更好的排序效果
            Collections.shuffle(tmpList);
            // 将broker按是否可用、消息发送延迟时间、开始可用时间排序， 效果是越靠前的越好
            Collections.sort(tmpList);

            // 取中间的数，为了下面只取前面一半
            final int half = tmpList.size() / 2;
            if (half <= 0) {
                // 只有一个
                return tmpList.get(0).getName();
            } else {
                // 只取列表中前面的一半broker
                final int i = this.whichItemWorst.incrementAndGet() % half;
                return tmpList.get(i).getName();
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return "LatencyFaultToleranceImpl{" +
            "faultItemTable=" + faultItemTable +
            ", whichItemWorst=" + whichItemWorst +
            '}';
    }

    /**
     * 封装上次不可用的broker
     */
    class FaultItem implements Comparable<FaultItem> {
        // broker名称
        private final String name;
        // 本次消息发送的延迟
        private volatile long currentLatency;
        // 可能再次可用的时间
        private volatile long startTimestamp;

        public FaultItem(final String name) {
            this.name = name;
        }

        @Override
        public int compareTo(final FaultItem other) {
            if (this.isAvailable() != other.isAvailable()) {
                if (this.isAvailable())
                    return -1;

                if (other.isAvailable())
                    return 1;
            }

            if (this.currentLatency < other.currentLatency)
                return -1;
            else if (this.currentLatency > other.currentLatency) {
                return 1;
            }

            if (this.startTimestamp < other.startTimestamp)
                return -1;
            else if (this.startTimestamp > other.startTimestamp) {
                return 1;
            }

            return 0;
        }

        /**
         * 是否可用，当前时间已经大于startTimestamp就可用
         */
        public boolean isAvailable() {
            // 如果当前时间大于再次可用时间，认为可用
            return (System.currentTimeMillis() - startTimestamp) >= 0;
        }

        @Override
        public int hashCode() {
            int result = getName() != null ? getName().hashCode() : 0;
            result = 31 * result + (int) (getCurrentLatency() ^ (getCurrentLatency() >>> 32));
            result = 31 * result + (int) (getStartTimestamp() ^ (getStartTimestamp() >>> 32));
            return result;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof FaultItem))
                return false;

            final FaultItem faultItem = (FaultItem) o;

            if (getCurrentLatency() != faultItem.getCurrentLatency())
                return false;
            if (getStartTimestamp() != faultItem.getStartTimestamp())
                return false;
            return getName() != null ? getName().equals(faultItem.getName()) : faultItem.getName() == null;

        }

        @Override
        public String toString() {
            return "FaultItem{" +
                "name='" + name + '\'' +
                ", currentLatency=" + currentLatency +
                ", startTimestamp=" + startTimestamp +
                '}';
        }

        public String getName() {
            return name;
        }

        public long getCurrentLatency() {
            return currentLatency;
        }

        public void setCurrentLatency(final long currentLatency) {
            this.currentLatency = currentLatency;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public void setStartTimestamp(final long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

    }
}
