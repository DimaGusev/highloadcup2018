package com.dgusev.hlcup2018.accountsapp.init;

import com.dgusev.hlcup2018.accountsapp.model.Account;
import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import com.dgusev.hlcup2018.accountsapp.netty.NioServer;
import com.dgusev.hlcup2018.accountsapp.parse.AccountParser;
import com.dgusev.hlcup2018.accountsapp.pool.ObjectPool;
import com.dgusev.hlcup2018.accountsapp.service.AccountConverter;
import com.dgusev.hlcup2018.accountsapp.service.AccountService;
import io.netty.channel.epoll.EpollServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@Component
public class DataLoader implements CommandLineRunner {

    @Autowired
    private AccountParser accountParser;

    @Value("${data.initial.file.win}")
    private String initFileWin;

    @Value("${data.initial.file.linux}")
    private String initFileLinux;

    @Value("${warmup.enabled:false}")
    private boolean warmUp;


    @Autowired
    private AccountService accountService;

    @Autowired
    private NowProvider nowProvider;

    @Autowired
    private AccountConverter accountConverter;

    @Autowired
    private NioServer nioServer;

    @Autowired
    private EpollServer epollServer;

    @Override
    public void run(String... args) throws Exception {
        try {

            Process p = Runtime.getRuntime().exec("uname -r");

            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(p.getInputStream()));
            String line = null;
            while ((line = stdInput.readLine()) != null) {
                System.out.println(line);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        String initFile = null;
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            initFile = initFileWin;
        } else {
            initFile = initFileLinux;
        }
        ZipFile zipFile = new ZipFile(initFile);
        TreeMap<Integer, ZipEntry> accountsFileTreeMap = new TreeMap<>();
        Collections.list(zipFile.entries()).forEach(zipEntry -> {
            try {
                if (zipEntry.getName().startsWith("accounts_")) {
                    String number = zipEntry.getName().substring(9);
                    accountsFileTreeMap.put(Integer.valueOf(number.substring(0, number.length() - 5)), zipEntry);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
        int now = new Scanner(new FileInputStream(new File(new File(initFile).getParentFile(), "options.txt") )).nextInt();
        nowProvider.initNow(now);
        System.out.println("Start load data" + new Date());
        Statistics statistics = new Statistics();
        int count = 0;
        int totalCount = accountsFileTreeMap.size();
        int loadCount = totalCount/4 ==0 ? 1 : totalCount/4;
        Map.Entry<Integer, ZipEntry>[] array = (Map.Entry<Integer, ZipEntry>[]) accountsFileTreeMap.entrySet().toArray(new Map.Entry[0]);
        int currentIndex = 0;
        List<ImporterCallable> importerCallables = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            int to = currentIndex + loadCount;
            if (i == 3) {
                to = array.length;
            }
            importerCallables.add(new ImporterCallable(zipFile, array, i, currentIndex, to));
            currentIndex = to;
        }
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        executorService.invokeAll(importerCallables).stream().map(f -> {
            try {
                return f.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        }).sorted(Comparator.comparingInt(r -> r.order)).forEach(r -> {
            for (Account acc: r.result) {
                statistics.analyze(acc);
                accountService.loadSequentially(acc);
            }
        });
        accountService.rearrange();
        System.out.println(statistics);
        accountService.finishLoad();
        ObjectPool.init();
        System.out.println("Indexes created " + new Date());
        new Thread(() -> {
            try {
                //nettyServer.start();
                if (System.getProperty("os.name").toLowerCase().contains("win")) {
                    nioServer.start();
                } else  {
                    epollServer.start();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        Thread.sleep(1000);
        if (warmUp) {
            System.out.println("Start warmup: " + new Date());
            Thread warmupThread = new Thread(() -> {
                try {
                    for (int i = 0; i < 3; i++) {
                        warmUp(10000);
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            });
            warmupThread.start();
            warmupThread.join();
            System.out.println("Finish warmup: " + new Date());
        }
        System.gc();

    }

    private static class ImportResult {
        public int order;
        public List<Account> result;
    }

    private class ImporterCallable implements Callable<ImportResult> {

        private Map.Entry<Integer, ZipEntry>[] array;
        private ZipFile zipFile;
        private int order;
        private int from;
        private int to;

        public ImporterCallable(ZipFile zipFile, Map.Entry<Integer, ZipEntry>[] array, int order, int from, int to) {
            this.array = array;
            this.order = order;
            this.from = from;
            this.to = to;
            this.zipFile = zipFile;
        }

        @Override
        public ImportResult call() throws Exception {
            byte[] buf = new byte[1000000];
            List<Account> accounts = new ArrayList<>();
            for (int i = from; i < to; i++) {
                Map.Entry<Integer, ZipEntry> entry = array[i];
                Integer n = entry.getKey();
                ZipEntry z = entry.getValue();
                try {
                    InputStream inputStream = zipFile.getInputStream(z);
                    inputStream.skip(14);
                    int openCount = 1;
                    while (openCount != 0) {
                        int ch = inputStream.read();
                        if (ch == ']') {
                            openCount--;
                        } else if (ch == '[') {
                            openCount++;
                        } else if (ch == '{') {
                            int brackets = 1;
                            int index = 0;
                            buf[index++] = '{';
                            while (brackets != 0) {
                                int ch2 = inputStream.read();
                                buf[index++] = (byte) ch2;
                                if (ch2 == '}') {
                                    brackets--;
                                } else if (ch2 == '{') {
                                    brackets++;
                                }
                            }
                            byte[] accountBytes = new byte[index];
                            System.arraycopy(buf, 0, accountBytes, 0, index);
                            AccountDTO accountDTO = accountParser.parse(accountBytes);
                            accounts.add(accountConverter.convert(accountDTO));

                        }

                    }
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            ImportResult importResult = new ImportResult();
            importResult.order = order;
            importResult.result = accounts;
            return importResult;
        }
    }

    private void warmUp(int limit) throws IOException {
        InputStream inputStream = new ClassPathResource("warmup.txt").getInputStream();
        int nextByte = 0;
        FastClient fastClient = new FastClient("localhost", 80);
        byte[] buf = new byte[1000];
        int totalCount = 0;
        while ((nextByte = inputStream.read()) != -1) {
            if (nextByte != 'G') {
                continue;
            }
            inputStream.read();
            inputStream.read();
            int space = inputStream.read();
            if (space != ' ') {
                continue;
            }
            int counter = 0;
            int rByte = 0;
            while ((rByte = inputStream.read()) != ' ') {
                buf[counter++] = (byte) rByte;
            }
            int startQueryIdPosition = -1;
            for (int i = 0; i < counter - QUERY_ID.length; i++) {
                if (buf[i] != QUERY_ID[0]) {
                    continue;
                }
                boolean equals = true;
                for (int j = 0; j < QUERY_ID.length; j++) {
                    if (QUERY_ID[j] != buf[i+j]) {
                        equals = false;
                        break;
                    }
                }
                if (equals) {
                    startQueryIdPosition = i;
                    break;
                }
            }
            if (startQueryIdPosition != -1) {
                int i = startQueryIdPosition;
                for (; i < counter; i++) {
                    if (buf[i] == '&') {
                        break;
                    }
                }
                if (i == counter) {
                    counter = startQueryIdPosition - 1;
                } else {
                    System.arraycopy(buf, i +1, buf, startQueryIdPosition, counter - i);
                    counter = counter - (i-startQueryIdPosition) - 1;
                }

            }
            try {
                Thread.sleep(10);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            fastClient.send(buf, 0, counter);
            if (totalCount++ > limit) {
                break;
            }
        }
        inputStream.close();
        fastClient.close();
    }

    private static final byte[] QUERY_ID = "query_id=".getBytes();
}
