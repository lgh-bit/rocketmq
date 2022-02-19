/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.common.message;

import java.nio.ByteBuffer;

/**
 * 扩展Message的Batch类型
 */
public class MessageExtBatch extends MessageExt {

    private static final long serialVersionUID = -2353110995348498537L;

    public ByteBuffer wrap() {
        assert getBody() != null;
        return ByteBuffer.wrap(getBody(), 0, getBody().length);
    }

    /**
     * 编码以后的Bytebuffer
     */
    private ByteBuffer encodedBuff;

    public ByteBuffer getEncodedBuff() {
        return encodedBuff;
    }

    public void setEncodedBuff(ByteBuffer encodedBuff) {
        this.encodedBuff = encodedBuff;
    }
}
