package com.dgusev.hlcup2018.accountsapp.netty;

import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetSocketAddress;
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

    private static final byte[] RESPONSE = "HTTP/1.0 200 OK\r\nConnection: keep-alive\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}".getBytes();

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

    public static class Worker implements Runnable {

        private Selector selector;
        private Queue<SocketChannel> queue = new ArrayDeque<>(20);

        public Worker() throws Exception {
            selector = Selector.open();
        }

        @Override
        public void run() {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(100000);
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
                                    int cnt = socketChannel.read(byteBuffer);
                                    if (cnt == -1) {
                                        socketChannel.close();
                                        selectionKey.cancel();
                                        continue;
                                    } else {
                                        //System.out.println(ATOMIC_INTEGER.incrementAndGet());
                                        byteBuffer.clear();
                                        byteBuffer.put(RESPONSE);
                                        byteBuffer.flip();
                                        socketChannel.write(byteBuffer);
                                        byteBuffer.clear();
                                    }
                                }
                            } catch (IOException ex) {
                                try {
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

        public void register(SocketChannel socketChannel) throws ClosedChannelException {
            queue.offer(socketChannel);
            selector.wakeup();
        }
    }

}
