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


import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

@Slf4j
public class NettyServerRpcConnection implements Closeable {

    /**
     * When the read or write buffer size is larger than this limit, i/o will be
     * done in chunks of this size. Most RPC requests and responses would be
     * be smaller.
     */
    private static int NIO_BUFFER_LIMIT = 8 * 1024; //should not be more than 64KB.
    public static final byte[] RPC_HEADER = new byte[]{'h', 'r', 'p', 'c'};
    public static final byte RPC_CURRENT_VERSION = 9;

    protected boolean connectionHeaderRead = false;

    private ByteBuffer unwrappedData;
    private ByteBuffer unwrappedDataLengthBuffer;

    private IpcConnectionContextProtos.IpcConnectionContextProto ipcConnectionContext;
    private RpcHeaderProtos.RpcRequestHeaderProto rpcRequestHeader;

    protected String hostAddress;
    protected int remotePort;
    protected InetAddress addr;
    private NettyRpcServer rpcServer;

    private void doBadPreambleHandling(String msg) throws IOException {
        doBadPreambleHandling(msg, new IOException(msg));
    }

//    protected final RpcResponse getErrorResponse(String msg, Exception e) throws IOException {
//        RpcHeaderProtos.RpcResponseHeaderProto.Builder headerBuilder = RpcHeaderProtos.RpcResponseHeaderProto.newBuilder().setCallId(-1);
////        ServerCall.setExceptionResponse(e, msg, headerBuilder);
//        ByteBuffer headerBuf =
//                ServerCall.createHeaderAndMessageBytes(null, headerBuilder.build(), 0, null);
//        BufferChain buf = new BufferChain(headerBuf);
//        return () -> buf;
//    }

    private void doBadPreambleHandling(String msg, Exception e) throws IOException {
        log.warn(msg);
//        doRespond(getErrorResponse(msg, e));
    }

    private String getFatalConnectionString(final int version, final byte authByte) {
        return "serverVersion=" + RPC_CURRENT_VERSION +
                ", clientVersion=" + version + ", authMethod=" + authByte +
                " from " + toString();
    }

    public boolean processPreamble(ByteBuffer preambleBuffer) throws IOException {
        //  Header, 4 bytes ("hrpc")
        for (int i = 0; i < RPC_HEADER.length; i++) {
            if (RPC_HEADER[i] != preambleBuffer.get()) {
                doBadPreambleHandling(
                        "Expected HEADER=" + new String(RPC_HEADER) + " but received HEADER=" +
                                Bytes.toStringBinary(preambleBuffer.array(), 0, RPC_HEADER.length) + " from " +
                                toString());
                return false;
            }
        }
        // Version, 1 byte (default verion 9)
        int version = preambleBuffer.get() & 0xFF;

        // RPC service class, 1 byte (0x00)
        byte rpcbyte = preambleBuffer.get();

        // Auth protocol, 1 byte (Auth method None = 0)
        byte authbyte = preambleBuffer.get();

        if (version != RPC_CURRENT_VERSION) {
            String msg = getFatalConnectionString(version, authbyte);
            doBadPreambleHandling(msg, new IOException(msg));
            return false;
        }

        // Length of the RpcRequestHeaderProto  + length of the IpcConnectionContextProto (4 bytes/32 bit int)
        int length = preambleBuffer.getInt();

        InputStream inputStream;
        if (preambleBuffer.hasArray()) {
            inputStream = UnsafeByteOperations.unsafeWrap(preambleBuffer.array(), 0, length).newInput();
        } else {
            inputStream = UnsafeByteOperations.unsafeWrap(preambleBuffer).newInput();
        }

        // Serialized delimited RpcRequestHeaderProto
//        this.rpcRequestHeader = RpcHeaderProtos.RpcRequestHeaderProto.parseFrom(inputStream);
        this.rpcRequestHeader = RpcHeaderProtos.RpcRequestHeaderProto.parseDelimitedFrom(inputStream);
        int callId = rpcRequestHeader.getCallId();
        int retry = rpcRequestHeader.getRetryCount();
        if (log.isDebugEnabled()) {
            log.debug(" got #" + callId);
        }

        // Serialized delimited IpcConnectionContextProto
        this.ipcConnectionContext = IpcConnectionContextProtos.IpcConnectionContextProto.parseDelimitedFrom(inputStream);

        return true;
    }

    @FunctionalInterface
    protected interface CallCleanup {
        void run();
    }

    protected CallCleanup callCleanup;

    final Channel channel;

    NettyServerRpcConnection(NettyRpcServer rpcServer, Channel channel) {
        this.unwrappedDataLengthBuffer = ByteBuffer.allocate(4);
        this.rpcServer = rpcServer;
        this.channel = channel;
        InetSocketAddress inetSocketAddress = ((InetSocketAddress) channel.remoteAddress());
        this.addr = inetSocketAddress.getAddress();
        if (addr == null) {
            this.hostAddress = "*Unknown*";
        } else {
            this.hostAddress = inetSocketAddress.getAddress().getHostAddress();
        }
        this.remotePort = inetSocketAddress.getPort();
    }

    void process(final ByteBuf buf) throws IOException, InterruptedException {
        if (connectionHeaderRead) {
            this.callCleanup = buf::release;
            process(buf.nioBuffer());
        } else {
            ByteBuffer connectionHeader = ByteBuffer.allocate(buf.readableBytes());
            buf.readBytes(connectionHeader);
            buf.release();
            process(connectionHeader);
        }
    }

    private void unwrapPacketAndProcessRpcs(byte[] inBuf)
            throws IOException, InterruptedException {
        ReadableByteChannel ch = Channels.newChannel(new ByteArrayInputStream(
                inBuf));
        // Read all RPCs contained in the inBuf, even partial ones
        while (true) {
            int count = -1;
            if (unwrappedDataLengthBuffer.remaining() > 0) {
                count = channelRead(ch, unwrappedDataLengthBuffer);
                if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0)
                    return;
            }

            if (unwrappedData == null) {
                unwrappedDataLengthBuffer.flip();
                int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();
                unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
            }

            count = channelRead(ch, unwrappedData);
            if (count <= 0 || unwrappedData.remaining() > 0)
                return;

            if (unwrappedData.remaining() == 0) {
                unwrappedDataLengthBuffer.clear();
                unwrappedData.flip();
                ByteBuffer requestData = unwrappedData;
                unwrappedData = null; // null out in case processOneRpc throws.
                processOneRpc(requestData);
            }
        }
    }

    private int channelRead(ReadableByteChannel channel,
                            ByteBuffer buffer) throws IOException {
        int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                channel.read(buffer) : channelIO(channel, null, buffer);
        return count;
    }

    private static int channelIO(ReadableByteChannel readCh,
                                 WritableByteChannel writeCh,
                                 ByteBuffer buf) throws IOException {

        int originalLimit = buf.limit();
        int initialRemaining = buf.remaining();
        int ret = 0;

        while (buf.remaining() > 0) {
            try {
                int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
                buf.limit(buf.position() + ioSize);

                ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf);

                if (ret < ioSize) {
                    break;
                }

            } finally {
                buf.limit(originalLimit);
            }
        }

        int nBytes = initialRemaining - buf.remaining();
        return (nBytes > 0) ? nBytes : ret;
    }

    void process(ByteBuffer buf) throws IOException, InterruptedException {
        try {
            processOneRpc(buf);
        } catch (Exception e) {
            if (callCleanup != null) {
                callCleanup.run();
            }
            throw e;
        } finally {
            this.callCleanup = null;
        }
    }

    public void processOneRpc(ByteBuffer buf) throws IOException, InterruptedException {
        if (connectionHeaderRead) {

        } else {
            this.connectionHeaderRead = true;
        }
    }

    @Override
    public synchronized void close() {
//        disposeSasl();
        channel.close();
        callCleanup = null;
    }
//
//    @Override
//    public boolean isConnectionOpen() {
//        return channel.isOpen();
//    }
//
//
//    @Override
//    protected void doRespond(RpcResponse resp) {
//        channel.writeAndFlush(resp);
//    }
}
