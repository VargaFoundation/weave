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
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;

@Slf4j
public class SocketHandler implements Runnable {
    public static final byte[] RPC_HEADER = new byte[]{'h', 'r', 'p', 'c'};
    public static final byte RPC_CURRENT_VERSION = 9;

    private Socket socket;
    private RpcPacketHandler rpcPacketHandler;
    private Map<Integer, PacketHandler> packetHandlers;
    private SocketContext socketContext;

    public SocketHandler(Socket socket, RpcPacketHandler rpcPacketHandler,
                         Map<Integer, PacketHandler> packetHandlers) {
        this.socket = socket;
        this.rpcPacketHandler = rpcPacketHandler;
        this.packetHandlers = packetHandlers;
        this.socketContext = new SocketContext();
        this.socketContext.setSocket(this.socket);
    }

    @Override
    public void run() {
        try {
            DataOutputStream out =
                    new DataOutputStream(this.socket.getOutputStream());
            DataInputStream in = new DataInputStream(this.socket.getInputStream());

            // read connection header
            byte[] connectionHeaderBuf = new byte[7];
            in.readFully(connectionHeaderBuf);

            // Validate connection header (hrpc, version, auth_protocol)
            //  Header, 4 bytes ("hrpc")
            for (int i = 0; i < RPC_HEADER.length; i++) {
                if (RPC_HEADER[i] != connectionHeaderBuf[i]) {
                    log.warn("Wrong header: {}", new String(connectionHeaderBuf));
                    return; // TODO
                }
            }
            // Version, 1 byte (default verion 9)
            int version = connectionHeaderBuf[4] & 0xFF;

            // RPC service class, 1 byte (0x00)
            byte rpcbyte = connectionHeaderBuf[5]; // TODO

            // Auth protocol, 1 byte (Auth method None = 0)
            byte authbyte = connectionHeaderBuf[6]; // TODO

            if (version != RPC_CURRENT_VERSION) {
                log.warn("Wrong version: {}", version);
                return; // TODO
            }

            while (true) {
                // read total packet length
                int packetLen = in.readInt();

                // parse rpc header
                RpcHeaderProtos.RpcRequestHeaderProto rpcRequestHeaderProto =
                        RpcHeaderProtos.RpcRequestHeaderProto.parseDelimitedFrom(in);

                int callId = rpcRequestHeaderProto.getCallId();
                if (callId >= 0) {
                    // rpc request
                    this.rpcPacketHandler.handle(in, out,
                            rpcRequestHeaderProto, this.socketContext);
                } else {
                    // should be handled by a registered packet handler
                    if (!this.packetHandlers.containsKey(callId)) {
                        throw new Exception("packet type with callId '" + callId
                                + "' not supported");
                    }

                    this.packetHandlers.get(callId).handle(in, out,
                            rpcRequestHeaderProto, this.socketContext);
                }
            }
        } catch (Exception e) {
            log.error("Failed to read rpc request", e);
        } finally {
            try {
                this.socket.close();
            } catch (IOException e) {
                log.error("Failed to close socket", e);
            }
        }
    }
}
