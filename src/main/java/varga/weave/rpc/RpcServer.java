package varga.weave.rpc;

/*-
 * #%L
 * Weave
 * %%
 * Copyright (C) 2025 - 2026 Varga Foundation
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

import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

@Slf4j
public class RpcServer extends Thread {
    private ServerSocket serverSocket;
    private ExecutorService executorService;
    private RpcPacketHandler rpcPacketHandler;
    private Map<Integer, PacketHandler> packetHandlers;

    public RpcServer(ServerSocket serverSocket,
                     ExecutorService executorService) {
        this.serverSocket = serverSocket;
        this.executorService = executorService;
        this.rpcPacketHandler = new RpcPacketHandler();
        this.packetHandlers = new HashMap<>();
    }

    public void addRpcProtocol(String className, Object protocol) {
        this.rpcPacketHandler.addProtocol(className, protocol);
    }

    public void addPacketHandler(PacketHandler packetHandler) {
        this.packetHandlers.put(packetHandler.getCallId(), packetHandler);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Socket socket = this.serverSocket.accept();

                // add SocketHandler to threadpool
                Runnable socketHandler = new SocketHandler(socket,
                        this.rpcPacketHandler, this.packetHandlers);
                this.executorService.execute(socketHandler);
            } catch (Exception e) {
                log.error("failed to accept server connection", e);
            }
        }
    }
}
