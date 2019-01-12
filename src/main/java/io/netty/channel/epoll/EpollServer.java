package io.netty.channel.epoll;

import com.dgusev.hlcup2018.accountsapp.pool.ObjectPool;
import com.dgusev.hlcup2018.accountsapp.service.AccountService;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import io.netty.channel.unix.FileDescriptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class EpollServer {


    private static final AtomicInteger REQUESTS = new AtomicInteger();

    @Autowired
    private RequestHandler requestHandler;

    public void start() throws Exception {
        boolean available = Epoll.isAvailable();
        if (!available) {
            throw new IllegalStateException("Native support is not available");
        }
        LinuxSocket serverSocket = LinuxSocket.newSocketStream();
        serverSocket.setTcpNoDelay(true);
        serverSocket.setSoLinger(0);
        serverSocket.setReusePort(true);
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(80));
        serverSocket.listen(10);
        FileDescriptor epollFd = Native.newEpollCreate();
        Native.epollCtlAdd(epollFd.intValue(), serverSocket.intValue(), Native.EPOLLIN | Native.EPOLLET);
        EpollEventArray epollEventArray = new EpollEventArray(10);
        FileDescriptor fakeDescriptor = new FileDescriptor(0);
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        Worker[] workerList = new Worker[4];
        for (int i = 0; i < 4; i++) {
            workerList[i] = new Worker();
            executorService.submit(workerList[i]);
        }
        int counter = 0;
        byte[] address = new byte[26];
        while (true) {
            int count = Native.epollWait(epollFd, epollEventArray, fakeDescriptor, -1, -1);
            if (count > 0) {
                int clientFd = -1;
                while ((clientFd = serverSocket.accept(address)) != -1) {
                    workerList[counter++ % 4].register(clientFd);
                }
            }
        }
    }


    public class Worker implements Runnable {

        private FileDescriptor epollFd;
        private EpollEventArray epollEventArray;
        private TIntObjectMap<LinuxSocket> clients;

        public Worker() throws Exception {
            epollFd = Native.newEpollCreate();
            epollEventArray = new EpollEventArray(4096);
            clients = new TIntObjectHashMap<>(10000);
        }

        @Override
        public void run() {
            ByteBuffer[] byteBuffers = new ByteBuffer[5];
            for (int i = 0; i < 5; i++) {
                byteBuffers[i] = ByteBuffer.allocateDirect(10000);
            }
            int counter = 0;
            byte[] buf = new byte[100000];
            FileDescriptor fakeDescriptor = new FileDescriptor(0);
            try {
                while (true) {
                    int count = Native.epollWait(epollFd, epollEventArray, fakeDescriptor, -1, -1);
                    if (count > 0) {
                        for (int i = 0; i < count; i++) {
                            int clientFd = epollEventArray.fd(i);
                            long event = epollEventArray.events(i);
                            LinuxSocket linuxSocket = clients.get(clientFd);
                            try {
                                if (linuxSocket != null) {
                                    if ((event & (Native.EPOLLERR | Native.EPOLLIN)) != 0) {
                                        ByteBuffer byteBuffer = byteBuffers[counter++ % 5];
                                        byteBuffer.clear();
                                        int cnt = linuxSocket.read(byteBuffer, byteBuffer.position(), byteBuffer.limit());
                                        if (cnt == -1) {
                                            if (RequestHandler.attachments.containsKey(clientFd)) {
                                                ObjectPool.releaseBuffer(RequestHandler.attachments.get(clientFd));
                                            }
                                            Native.epollCtlDel(epollFd.intValue(), clientFd);
                                            linuxSocket.close();
                                        } else if (cnt > 0) {
                                            byteBuffer.position(cnt);
                                            byteBuffer.flip();
                                            byteBuffer.get(buf, 0, byteBuffer.limit());
                                            byteBuffer.clear();
                                            requestHandler.handleRead(null, linuxSocket, buf, cnt, byteBuffer);
                                        }
                                    } else if ((event & Native.EPOLLRDHUP) != 0) {
                                        System.out.println("Connection reset fd=" + clientFd);
                                        if (RequestHandler.attachments.containsKey(clientFd)) {
                                            ObjectPool.releaseBuffer(RequestHandler.attachments.get(clientFd));
                                        }
                                        Native.epollCtlDel(epollFd.intValue(), clientFd);
                                        linuxSocket.close();
                                    } else {
                                        System.out.println("Unknown event from client fd=" + clientFd + " event=" + event);
                                    }
                                } else {
                                    if (RequestHandler.attachments.containsKey(clientFd)) {
                                        ObjectPool.releaseBuffer(RequestHandler.attachments.get(clientFd));
                                    }
                                    Native.epollCtlDel(epollFd.intValue(), clientFd);
                                    new LinuxSocket(clientFd).close();
                                }
                            } catch (Exception ex) {
                                ex.printStackTrace();
                                if (RequestHandler.attachments.containsKey(clientFd)) {
                                    ObjectPool.releaseBuffer(RequestHandler.attachments.get(clientFd));
                                }
                                Native.epollCtlDel(epollFd.intValue(), clientFd);
                                if (linuxSocket != null) {
                                    linuxSocket.close();
                                }
                            }
                        }
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        public void register(int clientFd) throws IOException {
            clients.put(clientFd, new LinuxSocket(clientFd));
            Native.epollCtlAdd(epollFd.intValue(), clientFd, Native.EPOLLIN | Native.EPOLLET | Native.EPOLLRDHUP);
        }
    }


}