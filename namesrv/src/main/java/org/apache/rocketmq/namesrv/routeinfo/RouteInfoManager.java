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
package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.remoting.common.RemotingUtil;

/**
 * 路由信息管理
 *
 * 基础设计：
 * 一个topic拥有过个消息队列
 * 一个broker为每个主题默认创建4个读队列和4个写队列
 * 多个broker组成一个集群
 * brokerName相同的多台broker组成maser-slave架构，brokerId为0的为master，大于0的为salve
 *
 */
public class RouteInfoManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    /**
     * 这是设置维持channel过期的时间，默认2分钟
     */
    private final static long BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;
    /**
     * 这里使用读写锁来控制并发
     * NameServer与broker建立长连接
     * 没收到一个心跳包会更新路由信息，更新时加上写锁
     * 允许多个producer并发读
     * 典型的读多写少的场景
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    // 消息队列路由信息
    private final HashMap<String/* topic */, List<QueueData>> topicQueueTable;
    //broker基础信息
    private final HashMap<String/* brokerName */, BrokerData> brokerAddrTable;
    // 集群信息
    private final HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;
    // broker状态信息
    private final HashMap<String/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;
    // broker上的FilterServer列表，用于类模式消息过滤
    private final HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

    public RouteInfoManager() {
        this.topicQueueTable = new HashMap<String, List<QueueData>>(1024);
        this.brokerAddrTable = new HashMap<String, BrokerData>(128);
        this.clusterAddrTable = new HashMap<String, Set<String>>(32);
        this.brokerLiveTable = new HashMap<String, BrokerLiveInfo>(256);
        this.filterServerTable = new HashMap<String, List<String>>(256);
    }

    /**
     * 获取全部的集群信息
     * @return return 封装的集群的信息
     */
    public byte[] getAllClusterInfo() {
        ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
        clusterInfoSerializeWrapper.setBrokerAddrTable(this.brokerAddrTable);
        clusterInfoSerializeWrapper.setClusterAddrTable(this.clusterAddrTable);
        return clusterInfoSerializeWrapper.encode();
    }

    /**
     * 删除一个topic
     */
    public void deleteTopic(final String topic) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                this.topicQueueTable.remove(topic);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        }
    }

    /**
     * 获取全部的topic
     */
    public byte[] getAllTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                topicList.getTopicList().addAll(this.topicQueueTable.keySet());
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    /**
     * 注册Broker
     * @param clusterName 集群名称
     * @param brokerAddr brokerAddr地址
     * @param backUpBrokerAddr backUpBrokerAddr broker的备用地址。
     * @param brokerName brokername
     * @param brokerId brokerId
     * @param haServerAddr 备节点的地址
     * @param topicConfigWrapper topic的配置信息
     * @param filterServerList filterServer
     * @param channel Netty的读写channel，
     * @return ;
     */
    public RegisterBrokerResult registerBroker(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final String haServerAddr,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final List<String> filterServerList,
        final Channel channel) {
        /*
         * 注册结果，如果是备节点，会返回它的master地址，否则result返回的字段全为null
         */
        RegisterBrokerResult result = new RegisterBrokerResult();
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                // 更新路由信息
                Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
                if (null == brokerNames) {
                    brokerNames = new HashSet<String>();
                    this.clusterAddrTable.put(clusterName, brokerNames);
                }
                brokerNames.add(brokerName);

                //是否是第一次注册
                boolean registerFirst = false;

                // 更新broker基础信息
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null == brokerData) {
                    //标记是第一次注册
                    registerFirst = true;
                    brokerData = new BrokerData(clusterName, brokerName, new HashMap<Long, String>());
                    this.brokerAddrTable.put(brokerName, brokerData);
                }
                Map<Long, String> brokerAddrsMap = brokerData.getBrokerAddrs();
                //Switch slave to master: first remove <1, IP:PORT> in namesrv, then add <0, IP:PORT>
                //The same IP:PORT must only have one record in brokerAddrTable
                Iterator<Entry<Long, String>> it = brokerAddrsMap.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<Long, String> item = it.next();
                    //broker地址相同但是brokerId不同，则移除
                    if (null != brokerAddr && brokerAddr.equals(item.getValue()) && brokerId != item.getKey()) {
                        it.remove();
                    }
                }

                //old地址
                String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
                //如果oldAddr是null，也说明是第一次注册
                registerFirst = registerFirst || (null == oldAddr);
                // 如果broker为master
                if (null != topicConfigWrapper
                    && MixAll.MASTER_ID == brokerId) {
                    // 如果broker topic发生了变化或者是首次注册
                    if (this.isBrokerTopicConfigChanged(brokerAddr, topicConfigWrapper.getDataVersion())
                        || registerFirst) {
                        ConcurrentMap<String, TopicConfig> tcTable =
                            topicConfigWrapper.getTopicConfigTable();
                        if (tcTable != null) {
                            for (Map.Entry<String, TopicConfig> entry : tcTable.entrySet()) {
                                //创建或者更新topic路由元数据
                                this.createAndUpdateQueueData(brokerName, entry.getValue());
                            }
                        }
                    }
                }

                /*
                 * 存活的Broker信息
                 */
                BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddr,
                    new BrokerLiveInfo(
                        System.currentTimeMillis(),
                        topicConfigWrapper.getDataVersion(),
                        channel,
                        haServerAddr));
                if (null == prevBrokerLiveInfo) {
                    log.info("new broker registered, {} HAServer: {}", brokerAddr, haServerAddr);
                }

                //注册broker的过滤器列表，一个broker会关联多个FilterServer消息过滤服务器
                if (filterServerList != null) {
                    if (filterServerList.isEmpty()) {
                        this.filterServerTable.remove(brokerAddr);
                    } else {
                        this.filterServerTable.put(brokerAddr, filterServerList);
                    }
                }

                //如果broker是slave节点
                if (MixAll.MASTER_ID != brokerId) {
                    //找到master的addr
                    String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (masterAddr != null) {
                        // 根据master addr找到master broker的状态信息
                        BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.get(masterAddr);
                        if (brokerLiveInfo != null) {
                            //更新broker的HaServerAddr和MasterAddr
                            //设置备节点地址
                            result.setHaServerAddr(brokerLiveInfo.getHaServerAddr());
                            //设置主节点地址
                            result.setMasterAddr(masterAddr);
                        }
                    }
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("registerBroker Exception", e);
        }

        return result;
    }

    /**
     * Broker的数据集是否有变化了
     * @param brokerAddr broker地址
     * @param dataVersion 数据版本
     * @return ;
     */
    public boolean isBrokerTopicConfigChanged(final String brokerAddr, final DataVersion dataVersion) {
        DataVersion prev = queryBrokerTopicConfig(brokerAddr);
        return null == prev || !prev.equals(dataVersion);
    }

    /**
     * 根据Broker地址获取BrokerLiveInfo，然后获取DataVersion
     */
    public DataVersion queryBrokerTopicConfig(final String brokerAddr) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (prev != null) {
            return prev.getDataVersion();
        }
        return null;
    }

    /**
     * 获取某个Broker地址的时间戳
     * @param brokerAddr broker地址
     */
    public void updateBrokerInfoUpdateTimestamp(final String brokerAddr) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (prev != null) {
            prev.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }

    /**
     * 创建或者更新Broker对应的队列的数据
     */
    private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        queueData.setPerm(topicConfig.getPerm());
        queueData.setTopicSysFlag(topicConfig.getTopicSysFlag());

        /*
         * 一个topic对应着多个broker
         */
        List<QueueData> queueDataList = this.topicQueueTable.get(topicConfig.getTopicName());
        if (null == queueDataList) {
            // 创建
            queueDataList = new LinkedList<QueueData>();
            queueDataList.add(queueData);
            this.topicQueueTable.put(topicConfig.getTopicName(), queueDataList);
            log.info("new topic registered, {} {}", topicConfig.getTopicName(), queueData);
        } else {
            // 更新
            boolean addNewOne = true;

            Iterator<QueueData> it = queueDataList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                // 如果是同一个broker
                if (qd.getBrokerName().equals(brokerName)) {
                    // 队列信息是否相同
                    if (qd.equals(queueData)) {
                        addNewOne = false;
                    } else {
                        log.info("topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), qd,
                            queueData);
                        // 移除旧的队列信息
                        it.remove();
                    }
                }
            }
            // 如果是新队列或者队列信息发生了变化
            if (addNewOne) {
                queueDataList.add(queueData);
            }
        }
    }

    //去除该broker上所有topic的写权限
    public int wipeWritePermOfBrokerByLock(final String brokerName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return wipeWritePermOfBroker(brokerName);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("wipeWritePermOfBrokerByLock Exception", e);
        }

        return 0;
    }

    //去除该broker上所有topic的写权限
    private int wipeWritePermOfBroker(final String brokerName) {
        int wipeTopicCnt = 0;
        Iterator<Entry<String, List<QueueData>>> itTopic = this.topicQueueTable.entrySet().iterator();
        while (itTopic.hasNext()) {
            Entry<String, List<QueueData>> entry = itTopic.next();
            List<QueueData> qdList = entry.getValue();

            Iterator<QueueData> it = qdList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                if (qd.getBrokerName().equals(brokerName)) {
                    int perm = qd.getPerm();
                    perm &= ~PermName.PERM_WRITE;
                    qd.setPerm(perm);
                    wipeTopicCnt++;
                }
            }
        }

        return wipeTopicCnt;
    }

    /**
     * 取消注册
     * @param clusterName 集群名称
     * @param brokerAddr brokerAddr地址
     * @param brokerName broker名称
     * @param brokerId 0是master节点
     */
    public void unregisterBroker(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.remove(brokerAddr);
                log.info("unregisterBroker, remove from brokerLiveTable {}, {}",
                    brokerLiveInfo != null ? "OK" : "Failed",
                    brokerAddr
                );

                this.filterServerTable.remove(brokerAddr);

                //移除结果，默认是false
                boolean removeBrokerName = false;
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    String addr = brokerData.getBrokerAddrs().remove(brokerId);
                    log.info("unregisterBroker, remove addr from brokerAddrTable {}, {}",
                        addr != null ? "OK" : "Failed",
                        brokerAddr
                    );

                    //如果broker对应的地址都是空里，则从brokerAddrTable里移除
                    if (brokerData.getBrokerAddrs().isEmpty()) {
                        this.brokerAddrTable.remove(brokerName);
                        log.info("unregisterBroker, remove name from brokerAddrTable OK, {}",
                            brokerName
                        );

                        removeBrokerName = true;
                    }
                }

                //如果完全移除了一个Broker，就从clusterAddrTable的列表移除该broker
                if (removeBrokerName) {
                    Set<String> nameSet = this.clusterAddrTable.get(clusterName);
                    if (nameSet != null) {
                        boolean removed = nameSet.remove(brokerName);
                        log.info("unregisterBroker, remove name from clusterAddrTable {}, {}",
                            removed ? "OK" : "Failed",
                            brokerName);

                        //如果nameSet为空，则移除整个clusterAddrTable的clusterName
                        if (nameSet.isEmpty()) {
                            this.clusterAddrTable.remove(clusterName);
                            log.info("unregisterBroker, remove cluster from clusterAddrTable {}",
                                clusterName
                            );
                        }
                    }
                    this.removeTopicByBrokerName(brokerName);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("unregisterBroker Exception", e);
        }
    }

    /**
     * 移除一个broker下的全部topic
     * @param brokerName ;
     */
    private void removeTopicByBrokerName(final String brokerName) {
        //topic的队列
        Iterator<Entry<String, List<QueueData>>> itMap = this.topicQueueTable.entrySet().iterator();
        while (itMap.hasNext()) {
            Entry<String, List<QueueData>> entry = itMap.next();

            String topic = entry.getKey();
            List<QueueData> queueDataList = entry.getValue();
            Iterator<QueueData> it = queueDataList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                //如果是这个broker的topic,就移除
                if (qd.getBrokerName().equals(brokerName)) {
                    log.info("removeTopicByBrokerName, remove one broker's topic {} {}", topic, qd);
                    it.remove();
                }
            }

            // 如果queueDataList为空了，就从把该topic的key移除
            if (queueDataList.isEmpty()) {
                log.info("removeTopicByBrokerName, remove the topic all queue {}", topic);
                itMap.remove();
            }
        }
    }

    /**
     * 获取某个topic的路由信息，BrokerData 和QueueData
     * @param topic tpic
     * @return TopicRouteData
     */
    public TopicRouteData pickupTopicRouteData(final String topic) {
        TopicRouteData topicRouteData = new TopicRouteData();
        //找到队列数据
        boolean foundQueueData = false;
        //找到Broker数据的标记
        boolean foundBrokerData = false;
        Set<String> brokerNameSet = new HashSet<String>();
        List<BrokerData> brokerDataList = new LinkedList<BrokerData>();
        topicRouteData.setBrokerDatas(brokerDataList);

        HashMap<String, List<String>> filterServerMap = new HashMap<String, List<String>>();
        topicRouteData.setFilterServerTable(filterServerMap);

        try {
            try {
                this.lock.readLock().lockInterruptibly();
                List<QueueData> queueDataList = this.topicQueueTable.get(topic);
                if (queueDataList != null) {
                    topicRouteData.setQueueDatas(queueDataList);
                    foundQueueData = true;

                    //构造BrokerNameSet
                    Iterator<QueueData> it = queueDataList.iterator();
                    while (it.hasNext()) {
                        QueueData qd = it.next();
                        brokerNameSet.add(qd.getBrokerName());
                    }

                    //获取BrokerDataList
                    for (String brokerName : brokerNameSet) {
                        BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                        if (null != brokerData) {
                            BrokerData brokerDataClone = new BrokerData(brokerData.getCluster(), brokerData.getBrokerName(), (HashMap<Long, String>) brokerData
                                .getBrokerAddrs().clone());
                            brokerDataList.add(brokerDataClone);
                            foundBrokerData = true;
                            for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                                List<String> filterServerList = this.filterServerTable.get(brokerAddr);
                                filterServerMap.put(brokerAddr, filterServerList);
                            }
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("pickupTopicRouteData Exception", e);
        }

        log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);

        //如果队列和Notify都找到了，就返回
        if (foundBrokerData && foundQueueData) {
            return topicRouteData;
        }

        return null;
    }

    /**
     * 扫描不活跃的Broker
     */
    public void scanNotActiveBroker() {
        Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, BrokerLiveInfo> next = it.next();
            long last = next.getValue().getLastUpdateTimestamp();
            // 超过120秒没有收到心跳，则认为broker下线
            if ((last + BROKER_CHANNEL_EXPIRED_TIME) < System.currentTimeMillis()) {
                // 关闭channel
                RemotingUtil.closeChannel(next.getValue().getChannel());
                // 移除该broker
                it.remove();
                log.warn("The broker channel expired, {} {}ms", next.getKey(), BROKER_CHANNEL_EXPIRED_TIME);
                this.onChannelDestroy(next.getKey(), next.getValue().getChannel());
            }
        }
    }

    /**
     * 通过关闭时触发此事件
     * @param remoteAddr ;
     * @param channel ;
     */
    public void onChannelDestroy(String remoteAddr, Channel channel) {
        String brokerAddrFound = null;
        if (channel != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    Iterator<Entry<String, BrokerLiveInfo>> itBrokerLiveTable =
                        this.brokerLiveTable.entrySet().iterator();
                    while (itBrokerLiveTable.hasNext()) {
                        Entry<String, BrokerLiveInfo> entry = itBrokerLiveTable.next();
                        if (entry.getValue().getChannel() == channel) {
                            brokerAddrFound = entry.getKey();
                            break;
                        }
                    }
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }

        if (null == brokerAddrFound) {
            brokerAddrFound = remoteAddr;
        } else {
            log.info("the broker's channel destroyed, {}, clean it's data structure at once", brokerAddrFound);
        }
        if (Objects.isNull(brokerAddrFound) || brokerAddrFound.length() <= 0) {
            return;
        }
        try {
            //加写锁
            this.lock.writeLock().lockInterruptibly();
            // 根据broker地址移除
            this.brokerLiveTable.remove(brokerAddrFound);
            this.filterServerTable.remove(brokerAddrFound);

            // 将该broker从brokerAddrTable的brokerData中移除
            String brokerNameFound = null;
            boolean removeBrokerName = false;
            Iterator<Entry<String, BrokerData>> itBrokerAddrTable =
                this.brokerAddrTable.entrySet().iterator();
            while (itBrokerAddrTable.hasNext() && (null == brokerNameFound)) {
                BrokerData brokerData = itBrokerAddrTable.next().getValue();

                Iterator<Entry<Long, String>> it = brokerData.getBrokerAddrs().entrySet().iterator();
                while (it.hasNext()) {
                    Entry<Long, String> entry = it.next();
                    Long brokerId = entry.getKey();
                    String brokerAddr = entry.getValue();
                    //如果brokerAddrFound相等，就移除
                    if (brokerAddr.equals(brokerAddrFound)) {
                        brokerNameFound = brokerData.getBrokerName();
                        it.remove();
                        log.info("remove brokerAddr[{}, {}] from brokerAddrTable, because channel destroyed",
                            brokerId, brokerAddr);
                        break;
                    }
                }

                //如果BrokerData的broker地址都被清空了，那就清空itBrokerAddrTable的此节点
                if (brokerData.getBrokerAddrs().isEmpty()) {
                    removeBrokerName = true;
                    itBrokerAddrTable.remove();
                    log.info("remove brokerName[{}] from brokerAddrTable, because channel destroyed",
                        brokerData.getBrokerName());
                }
            }

            // 将该broker从clusterAddrTable中移除
            if (brokerNameFound != null && removeBrokerName) {
                // cluster下的所有的brokerName
                Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<String, Set<String>> entry = it.next();
                    String clusterName = entry.getKey();
                    Set<String> brokerNames = entry.getValue();
                    boolean removed = brokerNames.remove(brokerNameFound);
                    if (removed) {
                        log.info("remove brokerName[{}], clusterName[{}] from clusterAddrTable, because channel destroyed",
                            brokerNameFound, clusterName);

                        if (brokerNames.isEmpty()) {
                            log.info("remove the clusterName[{}] from clusterAddrTable, because channel destroyed and no broker in this cluster",
                                clusterName);
                            it.remove();
                        }

                        break;
                    }
                }
            }

            //如果需要移除BrokerName
            if (removeBrokerName) {
                Iterator<Entry<String, List<QueueData>>> itTopicQueueTable =
                    this.topicQueueTable.entrySet().iterator();
                while (itTopicQueueTable.hasNext()) {
                    Entry<String, List<QueueData>> entry = itTopicQueueTable.next();
                    String topic = entry.getKey();
                    List<QueueData> queueDataList = entry.getValue();

                    Iterator<QueueData> itQueueData = queueDataList.iterator();
                    while (itQueueData.hasNext()) {
                        QueueData queueData = itQueueData.next();
                        if (queueData.getBrokerName().equals(brokerNameFound)) {
                            //移除这个Broker的队列
                            itQueueData.remove();
                            log.info("remove topic[{} {}], from topicQueueTable, because channel destroyed",
                                topic, queueData);
                        }
                    }

                    //如果一个topic下的队列都空了，就移除这个topic
                    if (queueDataList.isEmpty()) {
                        itTopicQueueTable.remove();
                        log.info("remove topic[{}] all queue, from topicQueueTable, because channel destroyed",
                            topic);
                    }
                }
            }
        } catch (Exception e) {
            log.error("onChannelDestroy Exception", e);
        }finally {
            this.lock.writeLock().unlock();
        }
    }

    /**
     * 打印全部的信息
     */
    public void printAllPeriodically() {
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                log.info("--------------------------------------------------------");
                {
                    log.info("topicQueueTable SIZE: {}", this.topicQueueTable.size());
                    Iterator<Entry<String, List<QueueData>>> it = this.topicQueueTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, List<QueueData>> next = it.next();
                        log.info("topicQueueTable Topic: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerAddrTable SIZE: {}", this.brokerAddrTable.size());
                    Iterator<Entry<String, BrokerData>> it = this.brokerAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, BrokerData> next = it.next();
                        log.info("brokerAddrTable brokerName: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerLiveTable SIZE: {}", this.brokerLiveTable.size());
                    Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, BrokerLiveInfo> next = it.next();
                        log.info("brokerLiveTable brokerAddr: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("clusterAddrTable SIZE: {}", this.clusterAddrTable.size());
                    Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, Set<String>> next = it.next();
                        log.info("clusterAddrTable clusterName: {} {}", next.getKey(), next.getValue());
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("printAllPeriodically Exception", e);
        }
    }

    /**
     * 获取系统的topiclist
     */
    public byte[] getSystemTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                for (Map.Entry<String, Set<String>> entry : clusterAddrTable.entrySet()) {
                    topicList.getTopicList().add(entry.getKey());
                    topicList.getTopicList().addAll(entry.getValue());
                }

                if (brokerAddrTable != null && !brokerAddrTable.isEmpty()) {
                    Iterator<String> it = brokerAddrTable.keySet().iterator();
                    while (it.hasNext()) {
                        BrokerData bd = brokerAddrTable.get(it.next());
                        HashMap<Long, String> brokerAddrs = bd.getBrokerAddrs();
                        if (brokerAddrs != null && !brokerAddrs.isEmpty()) {
                            Iterator<Long> it2 = brokerAddrs.keySet().iterator();
                            topicList.setBrokerAddr(brokerAddrs.get(it2.next()));
                            break;
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    /**
     * 根据cluster获取它下面的Topics
     * @param cluster 集群名称
     */
    public byte[] getTopicsByCluster(String cluster) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                //获取每一个broker下的topic
                Set<String> brokerNameSet = this.clusterAddrTable.get(cluster);
                for (String brokerName : brokerNameSet) {
                    Iterator<Entry<String, List<QueueData>>> topicTableIt =
                        this.topicQueueTable.entrySet().iterator();
                    while (topicTableIt.hasNext()) {
                        Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                        String topic = topicEntry.getKey();
                        List<QueueData> queueDatas = topicEntry.getValue();
                        for (QueueData queueData : queueDatas) {
                            if (brokerName.equals(queueData.getBrokerName())) {
                                topicList.getTopicList().add(topic);
                                break;
                            }
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    /**
     * 获取UnitTopics类型的flag
     * @return ;
     */
    public byte[] getUnitTopics() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt =
                    this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0
                        && TopicSysFlag.hasUnitFlag(queueDatas.get(0).getTopicSysFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    /**
     * 获取 getHasUnitSubTopicList
     * @return ;
     */
    public byte[] getHasUnitSubTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt =
                    this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0
                        && TopicSysFlag.hasUnitSubFlag(queueDatas.get(0).getTopicSysFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    /**
     * 是getUnitTopics 和getHasUnitSubTopicList 的交集
     * @return ;
     */
    public byte[] getHasUnitSubUnUnitTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt =
                    this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0
                        && !TopicSysFlag.hasUnitFlag(queueDatas.get(0).getTopicSysFlag())
                        && TopicSysFlag.hasUnitSubFlag(queueDatas.get(0).getTopicSysFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }
}

/**
 * 存活着Broker信息
 */
class BrokerLiveInfo {
    /**
     * 上次更新的时间戳
     */
    private long lastUpdateTimestamp;
    /**
     * 数据版本
     */
    private DataVersion dataVersion;
    /**
     * Netty的channel
     */
    private Channel channel;
    /**
     * 备节点地址
     */
    private String haServerAddr;

    public BrokerLiveInfo(long lastUpdateTimestamp, DataVersion dataVersion, Channel channel,
        String haServerAddr) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.dataVersion = dataVersion;
        this.channel = channel;
        this.haServerAddr = haServerAddr;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    @Override
    public String toString() {
        return "BrokerLiveInfo [lastUpdateTimestamp=" + lastUpdateTimestamp + ", dataVersion=" + dataVersion
            + ", channel=" + channel + ", haServerAddr=" + haServerAddr + "]";
    }
}
