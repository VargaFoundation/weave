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

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;

import java.io.DataInputStream;
import java.io.DataOutputStream;

@Slf4j
public class IpcConnectionContextPacketHandler implements PacketHandler {
    @Override
    public int getCallId() {
        return -3;
    }

    @Override
    public void handle(DataInputStream in, DataOutputStream out,
                       RpcHeaderProtos.RpcRequestHeaderProto rpcRequestHeaderProto,
                       SocketContext socketContext) throws Exception {

        IpcConnectionContextProtos.IpcConnectionContextProto context =
                IpcConnectionContextProtos.IpcConnectionContextProto
                        .parseDelimitedFrom(in);

        // update socket context
        log.info("Effective user: {}, real user: {}, protocol: {}", context.getUserInfo().getEffectiveUser(), context.getUserInfo().getRealUser(), context.getProtocol());
        socketContext.setEffectiveUser(context.getUserInfo().getEffectiveUser());
        socketContext.setRealUser(context.getUserInfo().getRealUser());
        socketContext.setProtocol(context.getProtocol());
    }
}
