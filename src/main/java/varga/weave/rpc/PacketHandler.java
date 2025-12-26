package varga.weave.rpc;

/*-
 * #%L
 * Weave
 * %%
 * Copyright (C) 2025 Varga Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public interface PacketHandler {
    int getCallId();

    void handle(DataInputStream in, DataOutputStream out,
                RpcHeaderProtos.RpcRequestHeaderProto rpcRequestHeaderProto,
                SocketContext socketContext) throws Exception;
}
