package io.netty.channel.epoll;

import com.dgusev.hlcup2018.accountsapp.pool.ObjectPool;
import com.dgusev.hlcup2018.accountsapp.service.AccountService;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import io.netty.channel.unix.FileDescriptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class EpollServer {


   //private static final AtomicLong ATOMIC_LONG = new AtomicLong();


    @Autowired
    private RequestHandler requestHandler;

    private Worker[] workerList;

    public void start() throws Exception {
        /*new Thread(()-> {
            while (true) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("T=" + ATOMIC_LONG.get());
            }
        }).start();*/
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
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        workerList = new Worker[3];
        for (int i = 0; i < 3; i++) {
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
                    workerList[counter++ % 3].register(clientFd);
                }
            }
        }
    }


    public class Worker implements Runnable {

        private FileDescriptor epollFd;
        private int suspendFd;
        private EpollEventArray epollEventArray;
        private TIntObjectMap<LinuxSocket> clients;

        public Worker() throws Exception {
            epollFd = Native.newEpollCreate();
            epollEventArray = new EpollEventArray(4096);
            clients = new TIntObjectHashMap<>(10000);
            suspendFd = Native.newEventFd().intValue();
            Native.epollCtlAdd(epollFd.intValue(), suspendFd, Native.EPOLLIN | Native.EPOLLET | Native.EPOLLRDHUP);
        }

        private volatile boolean suspended;

        @Override
        public void run() {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(10000);
            long address = getAddress(byteBuffer);
            byte[] buf = new byte[100000];
            FileDescriptor fakeDescriptor = new FileDescriptor(0);
            boolean suspended = false;
            try {
                while (true) {
                    int count = 0;
                    if (suspended) {
                        count =Native.epollWait(epollFd, epollEventArray, fakeDescriptor, -1, -1);
                    } else {
                        count = Native.epollBusyWait(epollFd, epollEventArray);
                    }
                    if (count > 0) {
                        if (suspended) {
                            suspended = false;
                        }
                        for (int i = 0; i < count; i++) {
                            int clientFd = epollEventArray.fd(i);
                            if (clientFd == suspendFd) {
                                System.out.println("Suspend start " + new Date());
                                suspended = true;
                                continue;
                            }
                            long event = epollEventArray.events(i);
                            LinuxSocket linuxSocket = clients.get(clientFd);
                            try {
                                if (linuxSocket != null) {
                                    if ((event & (Native.EPOLLERR | Native.EPOLLIN)) != 0) {
                                        byteBuffer.clear();
                                        int cnt = linuxSocket.read(byteBuffer, byteBuffer.position(), byteBuffer.limit());
                                        if (cnt == -1) {
                                            if (RequestHandler.attachments[clientFd] != null) {
                                                ObjectPool.releaseBuffer(RequestHandler.attachments[clientFd]);
                                                RequestHandler.attachments[clientFd] = null;
                                                RequestHandler.attachments = RequestHandler.attachments;
                                            }
                                            Native.epollCtlDel(epollFd.intValue(), clientFd);
                                            linuxSocket.close();
                                        } else if (cnt > 0) {
                                            byteBuffer.position(cnt);
                                            byteBuffer.flip();
                                            byteBuffer.get(buf, 0, byteBuffer.limit());
                                            byteBuffer.clear();
                                            //long t1 = System.nanoTime();
                                            requestHandler.handleRead(null, linuxSocket, buf, cnt, byteBuffer, address);
                                            //long t2 = System.nanoTime();
                                           // ATOMIC_LONG.addAndGet(t2-t1);
                                        }
                                    } else if ((event & Native.EPOLLRDHUP) != 0) {
                                        System.out.println("Connection reset fd=" + clientFd);
                                        if (RequestHandler.attachments[clientFd] != null) {
                                            ObjectPool.releaseBuffer(RequestHandler.attachments[clientFd]);
                                            RequestHandler.attachments[clientFd] = null;
                                            RequestHandler.attachments = RequestHandler.attachments;
                                        }
                                        Native.epollCtlDel(epollFd.intValue(), clientFd);
                                        linuxSocket.close();
                                    } else {
                                        System.out.println("Unknown event from client fd=" + clientFd + " event=" + event);
                                    }
                                } else {
                                    if (RequestHandler.attachments[clientFd] != null) {
                                        ObjectPool.releaseBuffer(RequestHandler.attachments[clientFd]);
                                        RequestHandler.attachments[clientFd] = null;
                                        RequestHandler.attachments = RequestHandler.attachments;
                                    }
                                    Native.epollCtlDel(epollFd.intValue(), clientFd);
                                    new LinuxSocket(clientFd).close();
                                }
                            } catch (Exception ex) {
                                ex.printStackTrace();
                                if (RequestHandler.attachments[clientFd] != null) {
                                    ObjectPool.releaseBuffer(RequestHandler.attachments[clientFd]);
                                    RequestHandler.attachments[clientFd] = null;
                                    RequestHandler.attachments = RequestHandler.attachments;
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
            RequestHandler.attachments[clientFd] = null;
            RequestHandler.attachments=RequestHandler.attachments;
            LinuxSocket linuxSocket = new LinuxSocket(clientFd);
            linuxSocket.setTcpNoDelay(true);
            //linuxSocket.setSoLinger(0);
            clients.put(clientFd, linuxSocket);
            Native.epollCtlAdd(epollFd.intValue(), clientFd, Native.EPOLLIN | Native.EPOLLET | Native.EPOLLRDHUP);
        }

        public void suspend() {
            Native.eventFdWrite(suspendFd, 1);
        }
    }


    private long getAddress(ByteBuffer byteBuffer) {
       try {
           Field field = Buffer.class.getDeclaredField("address");
           field.setAccessible(true);
           return (long)field.get(byteBuffer);
       } catch (Exception ex) {
           ex.printStackTrace();
           return 0;
       }
    }



    public void suspend() {
        if (workerList != null) {
            for (Worker worker : workerList) {
                worker.suspend();
            }
        }
    }

}
