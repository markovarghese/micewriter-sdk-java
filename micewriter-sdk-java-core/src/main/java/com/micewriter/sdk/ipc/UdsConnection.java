package com.micewriter.sdk.ipc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Persistent Netty connection to the micewriter-engine Unix Domain Socket.
 *
 * Thread-safe: multiple application threads may call {@link #send} concurrently.
 * Sends are serialised through a {@link ReentrantLock} because the UDS is a
 * single ordered stream — interleaving frames from different threads would corrupt
 * the protocol. ACKs arrive in the same order as sends.
 *
 * Linux only: uses Netty Epoll + {@code EpollDomainSocketChannel}.
 */
public class UdsConnection implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(UdsConnection.class);

    private final String socketPath;
    private final int connectTimeoutMs;
    private final int ackTimeoutMs;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final EpollEventLoopGroup group = new EpollEventLoopGroup(1);
    private volatile Channel channel;

    /** One slot per in-flight send; the response handler completes the future. */
    private final ConcurrentLinkedQueue<CompletableFuture<AckResponse>> ackFutures = new ConcurrentLinkedQueue<>();
    private final ReentrantLock sendLock = new ReentrantLock();

    public UdsConnection(String socketPath, int connectTimeoutMs, int ackTimeoutMs) {
        this.socketPath = socketPath;
        this.connectTimeoutMs = connectTimeoutMs;
        this.ackTimeoutMs = ackTimeoutMs;
        connect();
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Send a raw IPC payload (type discriminant byte + body) to the engine
     * and block until an ACK is received or {@code ackTimeoutMs} elapses.
     *
     * @param payload  1 byte message type + body bytes
     * @return         the engine's ACK response
     * @throws RuntimeException on timeout, channel error, or engine-reported error
     */
    public AckResponse send(byte[] payload) {
        int maxAttempts = 2;
        RuntimeException lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            CompletableFuture<AckResponse> future = new CompletableFuture<>();

            sendLock.lock();
            try {
                ensureConnected();

                // Frame = 4-byte big-endian length of payload + payload bytes.
                ByteBuf buf = Unpooled.buffer(4 + payload.length);
                buf.writeInt(payload.length);
                buf.writeBytes(payload);

                // Enqueue the ack-future and hand the frame to the channel under
                // the SAME lock. This guarantees ackFutures order matches the
                // wire order — if we offered the future before the lock, two
                // concurrent senders could queue futures in one order but write
                // payloads in the other, and AckHandler would complete each
                // future with the wrong sender's ACK.
                ackFutures.offer(future);
                try {
                    channel.writeAndFlush(buf);
                } catch (Exception writeEx) {
                    ackFutures.remove(future);
                    dropChannel();
                    lastException = new RuntimeException("IPC channel write failed", writeEx);
                    continue;
                }
            } catch (Exception e) {
                dropChannel();
                lastException = new RuntimeException("IPC channel setup failed", e);
                continue;
            } finally {
                sendLock.unlock();
            }

            try {
                return future.get(ackTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for ACK", e);
            } catch (TimeoutException e) {
                ackFutures.remove(future);
                dropChannel();
                lastException = new RuntimeException("micewriter-engine ACK timeout after " + ackTimeoutMs + "ms", e);
                log.warn("Send timed out, dropping channel (attempt {}/{})", attempt, maxAttempts);
            } catch (ExecutionException e) {
                dropChannel();
                lastException = new RuntimeException("IPC send failed", e.getCause());
                log.warn("Send failed, dropping channel (attempt {}/{})", attempt, maxAttempts);
            }
        }

        throw lastException != null ? lastException : new RuntimeException("IPC send failed after retries");
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.close().awaitUninterruptibly();
        }
        group.shutdownGracefully();
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private void connect() {
        Bootstrap b = new Bootstrap();
        b.group(group)
         .channel(EpollDomainSocketChannel.class)
         .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
         .handler(new ChannelInitializer<EpollDomainSocketChannel>() {
             @Override
             protected void initChannel(EpollDomainSocketChannel ch) {
                 ch.pipeline()
                   // Decode inbound ACK frames: 4-byte big-endian length, strip the header.
                   .addLast(new LengthFieldBasedFrameDecoder(64 * 1024, 0, 4, 0, 4))
                   .addLast(new AckHandler());
             }
         });

        ChannelFuture future = b.connect(new DomainSocketAddress(socketPath));
        boolean connected = future.awaitUninterruptibly(connectTimeoutMs, TimeUnit.MILLISECONDS);
        if (!connected || !future.isSuccess()) {
            String cause = future.cause() != null ? future.cause().getMessage() : "timeout";
            throw new RuntimeException("Failed to connect to micewriter-engine at " + socketPath + ": " + cause);
        }
        channel = future.channel();
        log.info("Connected to micewriter-engine at {}", socketPath);
    }

    private void ensureConnected() {
        if (channel == null || !channel.isActive()) {
            log.warn("UDS channel is not active — reconnecting");
            connect();
        }
    }

    private void dropChannel() {
        Channel c = channel;
        if (c != null) {
            c.close();
            channel = null;
        }
    }

    /** Netty inbound handler: deserialises each decoded ACK frame and queues it. */
    private class AckHandler extends SimpleChannelInboundHandler<ByteBuf> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            byte[] bytes = new byte[msg.readableBytes()];
            msg.readBytes(bytes);
            AckResponse ack = objectMapper.readValue(bytes, AckResponse.class);
            CompletableFuture<AckResponse> future = ackFutures.poll();
            if (future != null) {
                future.complete(ack);
            } else {
                log.warn("Received unexpected ACK from engine");
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("UDS channel error: {}", cause.getMessage());
            cancelPendingFutures(cause);
            dropChannel();
            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.warn("UDS channel closed by engine");
            cancelPendingFutures(new RuntimeException("UDS channel closed"));
            dropChannel();
        }

        private void cancelPendingFutures(Throwable cause) {
            CompletableFuture<AckResponse> f;
            while ((f = ackFutures.poll()) != null) {
                f.completeExceptionally(cause);
            }
        }
    }
}
