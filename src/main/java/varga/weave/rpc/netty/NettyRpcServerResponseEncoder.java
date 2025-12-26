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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

public class NettyRpcServerResponseEncoder extends ChannelOutboundHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception {
//        if (msg instanceof RpcResponse) {
//            RpcResponse resp = (NettyRpcProto.RpcResponse) msg;
//            BufferChain buf = resp.getResponse();
//            ctx.write(Unpooled.wrappedBuffer(buf.getBuffers()), promise).addListener(f -> {
//                resp.done();
//            });
//        } else {
//            ctx.write(msg, promise);
//        }
    }
}
