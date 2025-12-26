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
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;

import java.io.DataInputStream;
import java.io.DataOutputStream;

@Slf4j
public class SaslPacketHandler implements PacketHandler {
    @Override
    public int getCallId() {
        return -33;
    }

    @Override
    public void handle(DataInputStream in, DataOutputStream out,
                       RpcHeaderProtos.RpcRequestHeaderProto rpcRequestHeaderProto,
                       SocketContext socketContext) throws Exception {
        // handle SASL RPC request
        RpcHeaderProtos.RpcSaslProto rpcSaslProto =
                RpcHeaderProtos.RpcSaslProto.parseDelimitedFrom(in);

        log.info("rpcSaslProto - request:\n{}", ToStringBuilder.reflectionToString(rpcSaslProto));

        switch (rpcSaslProto.getState()) {
            case NEGOTIATE:
                // send automatic SUCCESS - mean simple server on HDFS side
                RpcHeaderProtos.RpcResponseHeaderProto rpcResponse =
                        RpcHeaderProtos.RpcResponseHeaderProto.newBuilder()
                                .setStatus(RpcStatusProto.SUCCESS)
                                .setCallId(rpcRequestHeaderProto.getCallId())
                                .setClientId(rpcRequestHeaderProto.getClientId())
                                .build();

                RpcHeaderProtos.RpcSaslProto message =
                        RpcHeaderProtos.RpcSaslProto.newBuilder()
                                .setState(RpcHeaderProtos.RpcSaslProto.SaslState.SUCCESS)
                                .build();

                // send response
                log.info("NEGOTIATE - response:\n{}\n{}", ToStringBuilder.reflectionToString(rpcResponse), ToStringBuilder.reflectionToString(message));
                RpcUtil.sendMessages(out, rpcResponse, message);
                break;
//            case CHALLENGE:
//            case SUCCESS:
            default:
                log.error("TODO - handle sasl " + rpcSaslProto.getState());
                break;
        }
    }
}
