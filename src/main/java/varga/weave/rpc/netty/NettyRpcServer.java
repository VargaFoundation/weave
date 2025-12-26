package varga.weave.rpc.netty;

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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.extern.slf4j.Slf4j;

import java.io.InterruptedIOException;
import java.net.InetSocketAddress;

@Slf4j
public class NettyRpcServer {

    public static void main(String... args) throws InterruptedIOException, InterruptedException {
        new NettyRpcServer();

    }

    volatile boolean running = true;
    private final Channel serverChannel;

    private final ChannelGroup allChannels =
            new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);

    /**
     * When the read or write buffer size is larger than this limit, i/o will be
     * done in chunks of this size. Most RPC requests and responses would be
     * be smaller.
     */
    private static int NIO_BUFFER_LIMIT = 8*1024; //should not be more than 64KB.

    private static final RecvByteBufAllocator IPC_RECVBUF_ALLOCATOR =
            new FixedRecvByteBufAllocator(NIO_BUFFER_LIMIT);

    public NettyRpcServer() throws InterruptedIOException, InterruptedException {

        InetSocketAddress bindAddress = new InetSocketAddress(8081);

        Class<? extends ServerChannel> channelClass;
        EventLoopGroup eventLoopGroup;
        int threadCount = 12;

        Boolean tcpNoDelay = true;
        Boolean tcpKeepAlive = true;
        int maxRequestSize = 1024 * 1024 * 1024 / 4;

        eventLoopGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("NettyRpcServer", true, Thread.MAX_PRIORITY));
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        channelClass = NioServerSocketChannel.class;

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(eventLoopGroup, workerGroup)
                .channel(channelClass)
                .childOption(ChannelOption.TCP_NODELAY, tcpNoDelay)
                .childOption(ChannelOption.SO_KEEPALIVE, tcpKeepAlive)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.RCVBUF_ALLOCATOR, IPC_RECVBUF_ALLOCATOR)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        ChannelPipeline pipeline = ch.pipeline();

//                        FixedLengthFrameDecoder preambleDecoder = new FixedLengthFrameDecoder(7);
//                        preambleDecoder.setSingleDecode(true);
//                        pipeline.addLast("preambleDecoder", preambleDecoder);

                        pipeline.addLast("preambleHandler", createNettyRpcServerPreambleHandler());
//                        pipeline.addLast("frameDecoder", new NettyRpcFrameDecoder(maxRequestSize));
                        pipeline.addLast("decoder", new NettyRpcServerRequestDecoder(allChannels));
                        pipeline.addLast("encoder", new NettyRpcServerResponseEncoder());
                    }
                });
        try {
            ChannelFuture f = bootstrap.bind(bindAddress).sync();
            serverChannel = f.channel();
            log.info("Bind to {}", serverChannel.localAddress());
            // Wait until the server socket is closed.
            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            throw new InterruptedIOException(e.getMessage());
        }
    }

    public synchronized void stop() {
        if (!running) {
            return;
        }
        log.info("Stopping server on " + this.serverChannel.localAddress());
        allChannels.close().awaitUninterruptibly();
        serverChannel.close();
        running = false;
    }

    protected NettyRpcServerPreambleHandler createNettyRpcServerPreambleHandler() {
        return new NettyRpcServerPreambleHandler(this);
    }
}
