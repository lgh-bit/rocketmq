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
package org.apache.rocketmq.remoting;

import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * RemotingClient
 */
public interface RemotingClient extends RemotingService {

    /**
     * 更新NameServer地址list
     */
    void updateNameServerAddressList(final List<String> addrs);

    /**
     * 获取NamesServerList
     */
    List<String> getNameServerAddressList();

    /**
     * 同步执行
     * @param addr addr
     * @param request request
     * @param timeoutMillis 超时时间
     */
    RemotingCommand invokeSync(final String addr, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException;

    /**
     * 异步执行
     * @param addr addr
     * @param request request
     * @param timeoutMillis 超时时间
     * @param invokeCallback 异步回调勾子
     */
    void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * oneWay执行
     * @param addr addr
     * @param request request
     * @param timeoutMillis 超时
     */
    void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException,
        RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 注册Processor
     * @param requestCode 请求code
     * @param processor procrsssor
     * @param executor 线程池
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
        final ExecutorService executor);

    /**
     * 设置callback线程池
     */
    void setCallbackExecutor(final ExecutorService callbackExecutor);

    /**
     * 获取callback线程池
     */
    ExecutorService getCallbackExecutor();

    /**
     * 通道是否可写
     */
    boolean isChannelWritable(final String addr);
}
