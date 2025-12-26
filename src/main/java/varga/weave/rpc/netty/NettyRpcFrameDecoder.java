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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

@Slf4j
public class NettyRpcFrameDecoder extends ByteToMessageDecoder {

    private static int FRAME_LENGTH_FIELD_LENGTH = 4;

    private final int maxFrameLength;

    public NettyRpcFrameDecoder(int maxFrameLength) {
        this.maxFrameLength = maxFrameLength;
    }

    private NettyServerRpcConnection connection;

    void setConnection(NettyServerRpcConnection connection) {
        this.connection = connection;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
            throws Exception {

        if (in.readableBytes() < FRAME_LENGTH_FIELD_LENGTH) {
            return;
        }

        long frameLength = in.getUnsignedInt(in.readerIndex());

        if (frameLength < 0) {
            throw new IOException("negative frame length field: " + frameLength);
        }

        int frameLengthInt = (int) frameLength;
        if (in.readableBytes() < frameLengthInt + FRAME_LENGTH_FIELD_LENGTH) {
            return;
        }

        in.skipBytes(FRAME_LENGTH_FIELD_LENGTH);

        // extract frame
        out.add(in.readRetainedSlice(frameLengthInt));
    }

}
