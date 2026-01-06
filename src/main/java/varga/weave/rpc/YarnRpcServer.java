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

import varga.weave.rpc.protocol.YarnApplicationClientProtocolService;
import varga.weave.rpc.protocol.YarnApplicationMasterProtocolService;
import varga.weave.rpc.protocol.YarnContainerManagementProtocolService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// https://snakebite.readthedocs.io/en/latest/hadoop_rpc.html
// https://github.com/yassineazzouz/cerastes/blob/90ceef51e399e319e6a94cb5e950f47dacf8821a/cerastes/channel.py#L308
@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class YarnRpcServer {

    private final YarnApplicationClientProtocolService yarnApplicationClientProtocolService;
    private final YarnApplicationMasterProtocolService yarnApplicationMasterProtocolService;
    private final YarnContainerManagementProtocolService yarnContainerManagementProtocolService;

    @PostConstruct
    public void init() throws IOException {

        // initialize rpc server
        ServerSocket serverSocket = new ServerSocket(10081); // TODO dynamic port
        log.warn("YARN RPC Server listening on {}:{}", serverSocket.getInetAddress(), serverSocket.getLocalPort());
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        RpcServer rpcServer = new RpcServer(serverSocket, executorService);

        rpcServer.addRpcProtocol(
                "org.apache.hadoop.yarn.api.ApplicationClientProtocolPB",
                this.yarnApplicationClientProtocolService);
        rpcServer.addRpcProtocol(
                "org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB",
                this.yarnApplicationMasterProtocolService);
        rpcServer.addRpcProtocol(
                "org.apache.hadoop.yarn.api.ContainerManagementProtocolPB",
                this.yarnContainerManagementProtocolService);

        rpcServer.addPacketHandler(new IpcConnectionContextPacketHandler());
        rpcServer.addPacketHandler(new SaslPacketHandler());

        rpcServer.start();
        log.warn("YARN RPC Server started");

        // wait until rpcServer shuts down
//        rpcServer.join();
    }
}
