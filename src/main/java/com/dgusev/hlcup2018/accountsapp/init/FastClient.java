package com.dgusev.hlcup2018.accountsapp.init;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.Date;

public class FastClient {

    private static final byte[] START = "GET ".getBytes();
    private static final byte[] FINISH = " HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".getBytes();


    private SocketChannel socketChannel;
    private ByteBuffer sendBuffer = ByteBuffer.allocate(1000);
    private Thread readThread;

    public FastClient(String host, int port) throws IOException {
        socketChannel = SocketChannel.open(new InetSocketAddress(port));
        readThread = new Thread(() -> {
            try {
                //byte[] buf = new byte[100000];
                ByteBuffer readBuffer = ByteBuffer.allocate(100000);
                while (true) {
                    if (Thread.interrupted()) {
                        break;
                    }
                    readBuffer.clear();
                    socketChannel.read(readBuffer);
                    //readBuffer.flip();
                    //readBuffer.get(buf, 0, readBuffer.limit());
                    //System.out.println(new String(buf, 0, readBuffer.limit()));
                }
            } catch (Exception ex) {
                if (ex instanceof AsynchronousCloseException || ex instanceof ClosedChannelException) {
                    System.out.println("Client closed " + new Date());
                } else {
                    ex.printStackTrace();
                }
            }
        });
        readThread.start();
    }

    public void send(byte[] request, int from, int to) throws IOException {
        sendBuffer.clear();
        sendBuffer.put(START);
        sendBuffer.put(request, from, to - from);
        sendBuffer.put(FINISH);
        sendBuffer.flip();
        socketChannel.write(sendBuffer);
    }

    public void close() throws IOException {
        socketChannel.close();
        readThread.interrupt();
    }


}