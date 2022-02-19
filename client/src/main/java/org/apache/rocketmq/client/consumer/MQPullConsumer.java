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
package org.apache.rocketmq.client.consumer;

import java.util.Set;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 *  MqPullConsumer。拉取消费
 *
 * Pulling consumer interface
 */
public interface MQPullConsumer extends MQConsumer {
    /**
     * Start the consumer
     */
    void start() throws MQClientException;

    /**
     * Shutdown the consumer
     */
    void shutdown();

    /**
     * Register the message queue listener
     *
     * 注册MessageQueueListener
     */
    void registerMessageQueueListener(final String topic, final MessageQueueListener listener);

    /**
     * 拉取消息
     * Pulling the messages,not blocking
     *
     * @param mq from which message queue
     * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br> if
     * null or * expression,meaning subscribe
     * all
     * @param offset from where to pull
     * @param maxNums max pulling numbers
     * @return The resulting {@code PullRequest}
     */
    PullResult pull(final MessageQueue mq, final String subExpression, final long offset,
        final int maxNums) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException;

    /**
     * 拉取消息
     * Pulling the messages in the specified timeout
     *
     * @return The resulting {@code PullRequest}
     */
    PullResult pull(final MessageQueue mq, final String subExpression, final long offset,
        final int maxNums, final long timeout) throws MQClientException, RemotingException,
        MQBrokerException, InterruptedException;

    /**
     * Pulling the messages, not blocking
     * <p>
     * support other message selection, such as {@link org.apache.rocketmq.common.filter.ExpressionType#SQL92}
     * </p>
     *
     * @param mq from which message queue
     * @param selector message selector({@link MessageSelector}), can be null.
     * @param offset from where to pull
     * @param maxNums max pulling numbers
     * @return The resulting {@code PullRequest}
     */
    PullResult pull(final MessageQueue mq, final MessageSelector selector, final long offset,
        final int maxNums) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException;

    /**
     * Pulling the messages in the specified timeout
     * <p>
     * support other message selection, such as {@link org.apache.rocketmq.common.filter.ExpressionType#SQL92}
     * </p>
     *
     * @param mq from which message queue
     * @param selector message selector({@link MessageSelector}), can be null.
     * @param offset from where to pull
     * @param maxNums max pulling numbers
     * @param timeout Pulling the messages in the specified timeout
     * @return The resulting {@code PullRequest}
     */
    PullResult pull(final MessageQueue mq, final MessageSelector selector, final long offset,
        final int maxNums, final long timeout) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException;

    /**
     * 异步拉取消息，在pullCallback里运行
     *
     * Pulling the messages in a async. way
     */
    void pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums,
        final PullCallback pullCallback) throws MQClientException, RemotingException,
        InterruptedException;

    /**
     *  异步拉取消息，在pullCallback里运行。带超时时间
     *
     * Pulling the messages in a async. way
     */
    void pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums,
        final PullCallback pullCallback, long timeout) throws MQClientException, RemotingException,
        InterruptedException;

    /**
     * Pulling the messages in a async. way. Support message selection
     */
    void pull(final MessageQueue mq, final MessageSelector selector, final long offset, final int maxNums,
        final PullCallback pullCallback) throws MQClientException, RemotingException,
        InterruptedException;

    /**
     * Pulling the messages in a async. way. Support message selection
     */
    void pull(final MessageQueue mq, final MessageSelector selector, final long offset, final int maxNums,
        final PullCallback pullCallback, long timeout) throws MQClientException, RemotingException,
        InterruptedException;

    /**
     * 拉取消息，如果没有，就阻塞
     *
     * Pulling the messages,if no message arrival,blocking some time
     *
     * @return The resulting {@code PullRequest}
     */
    PullResult pullBlockIfNotFound(final MessageQueue mq, final String subExpression,
        final long offset, final int maxNums) throws MQClientException, RemotingException,
        MQBrokerException, InterruptedException;

    /**
     * Pulling the messages through callback function,if no message arrival,blocking.
     */
    void pullBlockIfNotFound(final MessageQueue mq, final String subExpression, final long offset,
        final int maxNums, final PullCallback pullCallback) throws MQClientException, RemotingException,
        InterruptedException;

    /**
     *  更新消费偏移量
     *
     * Update the offset
     */
    void updateConsumeOffset(final MessageQueue mq, final long offset) throws MQClientException;

    /**
     * 拉取消费者偏移量
     *
     * Fetch the offset
     *
     * @return The fetched offset of given queue
     */
    long fetchConsumeOffset(final MessageQueue mq, final boolean fromStore) throws MQClientException;

    /**
     * 拉取消息的队列inbalance
     *
     * Fetch the message queues according to the topic
     *
     * @param topic message topic
     * @return message queue set
     */
    Set<MessageQueue> fetchMessageQueuesInBalance(final String topic) throws MQClientException;

    /**
     * 消息失败发回broker
     *
     * If consuming failure,message will be send back to the broker,and delay consuming in some time later.<br>
     * Mind! message can only be consumed in the same group.
     */
    void sendMessageBack(MessageExt msg, int delayLevel, String brokerName, String consumerGroup)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

}
