package com.micewriter.sdk.ipc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Minimal in-process fake of the micewriter-engine UDS server for unit tests.
 *
 * Decodes 4-byte-length-prefixed frames, tracks received payloads, and sends
 * {@code {"status":"ok"}} ACKs via a dedicated executor thread (so the Netty
 * event loop is never blocked by test delays or gates).
 *
 * Use {@link #holdAcks()} / {@link #releaseAcks(int)} to gate ACK delivery
 * for backpressure tests, and {@link #awaitFrames(int, long, TimeUnit)} to
 * wait deterministically for a given number of received frames.
 */
class FakeUdsEngine implements AutoCloseable {

    private static final byte[] ACK_OK =
        "{\"status\":\"ok\"}".getBytes(StandardCharsets.UTF_8);

    private final String socketPath;
    private final long ackDelayMs;
    private final boolean sendAck;

    private final EpollEventLoopGroup bossGroup = new EpollEventLoopGroup(1);
    private final EpollEventLoopGroup workerGroup = new EpollEventLoopGroup(2);
    private final ExecutorService ackExecutor = Executors.newCachedThreadPool();
    private final Channel serverChannel;

    // Permit-based gate: drain to hold all ACKs, release(n) to unblock n of them.
    private final Semaphore ackGate;

    private final AtomicInteger receivedCount = new AtomicInteger();
    private final List<byte[]> receivedPayloads = new CopyOnWriteArrayList<>();
    private final AtomicInteger acksToSkip = new AtomicInteger(0);

    FakeUdsEngine(String socketPath) throws InterruptedException {
        this(socketPath, 0, true, Integer.MAX_VALUE);
    }

    FakeUdsEngine(String socketPath, long ackDelayMs) throws InterruptedException {
        this(socketPath, ackDelayMs, true, Integer.MAX_VALUE);
    }

    /** @param initialAckPermits pass 0 to start with all ACKs held; call releaseAcks() to unblock */
    FakeUdsEngine(String socketPath, long ackDelayMs, boolean sendAck, int initialAckPermits)
            throws InterruptedException {
        this.socketPath = socketPath;
        this.ackDelayMs = ackDelayMs;
        this.sendAck = sendAck;
        this.ackGate = new Semaphore(initialAckPermits);

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
         .channel(EpollServerDomainSocketChannel.class)
         .childHandler(new ChannelInitializer<Channel>() {
             @Override
             protected void initChannel(Channel ch) {
                 ch.pipeline()
                   .addLast(new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4))
                   .addLast(new FrameHandler());
             }
         });

        serverChannel = b.bind(new DomainSocketAddress(socketPath)).sync().channel();
    }

    // -------------------------------------------------------------------------
    // Test API
    // -------------------------------------------------------------------------

    int receivedCount() { return receivedCount.get(); }

    List<byte[]> receivedPayloads() { return receivedPayloads; }

    /** Hold all subsequent ACKs until {@link #releaseAcks} is called. */
    void holdAcks() { ackGate.drainPermits(); }

    /** Release n pending ACK permits, unblocking that many held ACK tasks. */
    void releaseAcks(int n) { ackGate.release(n); }

    /**
     * Cause the next {@code n} received frames to receive no ACK.
     * The client's ackTimeoutMs will fire, drop the channel, and complete
     * the send future exceptionally — simulating a real timeout for retry tests.
     */
    void skipNextAcks(int n) { acksToSkip.set(n); }

    /** Block until at least {@code count} frames have been received, or the timeout elapses. */
    boolean awaitFrames(int count, long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        while (receivedCount.get() < count) {
            if (System.currentTimeMillis() >= deadline) return false;
            Thread.sleep(5);
        }
        return true;
    }

    static String tmpSocketPath() {
        return "/tmp/micewriter-test-" + UUID.randomUUID() + ".sock";
    }

    @Override
    public void close() {
        // shutdownNow() interrupts tasks blocked on ackGate.acquire() or Thread.sleep,
        // causing their InterruptedException handler to exit cleanly — no overflow risk.
        ackExecutor.shutdownNow();
        serverChannel.close().awaitUninterruptibly();
        bossGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS).awaitUninterruptibly();
        workerGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS).awaitUninterruptibly();
        try { Files.deleteIfExists(Path.of(socketPath)); } catch (Exception ignored) {}
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private class FrameHandler extends SimpleChannelInboundHandler<ByteBuf> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
            byte[] bytes = new byte[msg.readableBytes()];
            msg.readBytes(bytes);
            receivedPayloads.add(bytes);
            receivedCount.incrementAndGet();

            if (sendAck) {
                // Handle ACK on a separate thread so the Netty event loop is never blocked
                // by ackDelayMs or ackGate waits.
                ackExecutor.submit(() -> {
                    try {
                        ackGate.acquire();
                        // If skipNextAcks(n) was called, silently drop this ACK.
                        // The client's ackTimeoutMs will fire and complete the future exceptionally.
                        if (acksToSkip.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) return;
                        if (ackDelayMs > 0) Thread.sleep(ackDelayMs);
                        ByteBuf response = Unpooled.buffer(4 + ACK_OK.length);
                        response.writeInt(ACK_OK.length);
                        response.writeBytes(ACK_OK);
                        ctx.writeAndFlush(response);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
        }
    }
}
