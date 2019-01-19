package com.dgusev.hlcup2018.accountsapp.init;

import com.dgusev.hlcup2018.accountsapp.model.Account;
import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import com.dgusev.hlcup2018.accountsapp.netty.NettyServer;
import com.dgusev.hlcup2018.accountsapp.netty.NioServer;
import com.dgusev.hlcup2018.accountsapp.parse.AccountParser;
import com.dgusev.hlcup2018.accountsapp.pool.ObjectPool;
import com.dgusev.hlcup2018.accountsapp.service.AccountConverter;
import com.dgusev.hlcup2018.accountsapp.service.AccountService;
import io.netty.channel.epoll.EpollServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
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
    private NettyServer nettyServer;

    @Autowired
    private AccountParser accountParser;

    @Value("${data.initial.file.win}")
    private String initFileWin;

    @Value("${data.initial.file.linux}")
    private String initFileLinux;


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
        System.gc();

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
                            if (System.getProperty("os.name").toLowerCase().contains("win") && initFileWin.endsWith("data2.zip")) {
                                if (i > 3) {
                                    accountDTO.id = 10000 * (i-1) + accountDTO.id;
                                    accountDTO.email = i + accountDTO.email;
                                    if (accountDTO.phone != null) {
                                        accountDTO.phone = i + accountDTO.phone;
                                    }
                                    if (accountDTO.likes != null) {
                                        for (int j = 0; j < accountDTO.likes.length; j++) {
                                            int id = (int)(accountDTO.likes[j]>>32);
                                            id = 10000 * (i-1) + id;
                                            accountDTO.likes[j] = 0xffffffffL & accountDTO.likes[j];
                                            accountDTO.likes[j] = accountDTO.likes[j] | ((long)id << 32);
                                        }
                                    }
                                }
                            }
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
}
