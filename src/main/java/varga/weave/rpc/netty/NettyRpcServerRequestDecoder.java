package varga.weave.rpc.netty;

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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyRpcServerRequestDecoder extends ChannelInboundHandlerAdapter {

    private final ChannelGroup allChannels;

    public NettyRpcServerRequestDecoder(ChannelGroup allChannels) {
        this.allChannels = allChannels;
    }

    private NettyServerRpcConnection connection;

    void setConnection(NettyServerRpcConnection connection) {
        this.connection = connection;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        allChannels.add(ctx.channel());
        log.trace("Connection {}; # active connections={}",
                ctx.channel().remoteAddress(), (allChannels.size() - 1));
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf input = (ByteBuf) msg;
        // 4 bytes length field
        connection.process(input);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        allChannels.remove(ctx.channel());
        log.trace("Disconnection {}; # active connections={}",
                ctx.channel().remoteAddress(), (allChannels.size() - 1));
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        allChannels.remove(ctx.channel());
        log.trace("Connection {}; caught unexpected downstream exception.",
                ctx.channel().remoteAddress(), e);
        ctx.channel().close();
    }
}
