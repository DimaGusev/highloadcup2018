package com.dgusev.hlcup2018.accountsapp.netty;

import io.netty.util.ReferenceCountUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class NioServer {

    private static final AtomicInteger ATOMIC_INTEGER = new AtomicInteger();

    public void start() throws Exception {
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        Worker[] workerList = new Worker[4];
        for (int i = 0; i < 4; i++) {
            workerList[i] = new Worker();
            executorService.submit(workerList[i]);
        }
        int counter = 0;
        serverSocketChannel.bind(new InetSocketAddress(80), 10);
        while (true) {
            SocketChannel socketChannel = serverSocketChannel.accept();
            socketChannel.configureBlocking(false);
            workerList[counter++ % 4].register(socketChannel);
        }
    }

    @Autowired
    private RequestHandler requestHandler;

    public class Worker implements Runnable {

        private Selector selector;
        private Queue<SocketChannel> queue = new ArrayDeque<>(20);

        public Worker() throws Exception {
            selector = Selector.open();
        }

        @Override
        public void run() {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(10000);
            byte[] buf = new byte[100000];
            try {
                while (true) {
                    int count = selector.select();
                    if (!queue.isEmpty()) {
                        queue.poll().register(selector, SelectionKey.OP_READ);
                    }
                    if (count != 0) {
                        Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                        while (iterator.hasNext()) {
                            SelectionKey selectionKey = iterator.next();
                            SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
                            try {
                                if (selectionKey.isReadable()) {
                                    byteBuffer.clear();
                                    int cnt = socketChannel.read(byteBuffer);
                                    if (cnt == -1) {
                                        if (selectionKey.attachment() != null) {
                                            ReferenceCountUtil.release(selectionKey.attachment());
                                        }
                                        socketChannel.close();
                                        selectionKey.cancel();
                                        continue;
                                    } else {
                                        byteBuffer.flip();
                                        byteBuffer.get(buf, 0, cnt);
                                        byteBuffer.clear();
                                        requestHandler.handleRead(selectionKey, buf, cnt, byteBuffer);

                                    }
                                }
                            } catch (Exception ex) {
                                try {
                                    if (selectionKey.attachment() != null) {
                                        ReferenceCountUtil.release(selectionKey.attachment());
                                    }
                                    selectionKey.cancel();
                                    socketChannel.close();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                continue;
                            } finally {
                                iterator.remove();
                            }

                        }
                    }


                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        public void register(SocketChannel socketChannel) throws IOException {
            socketChannel.setOption(StandardSocketOptions.SO_LINGER, 0);
            queue.offer(socketChannel);
            selector.wakeup();
        }
    }

}
