package com.dgusev.hlcup2018.accountsapp.init;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import com.dgusev.hlcup2018.accountsapp.netty.NettyServer;
import com.dgusev.hlcup2018.accountsapp.parse.AccountParser;
import com.dgusev.hlcup2018.accountsapp.service.AccountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@Component
public class DataLoader implements CommandLineRunner {

    @Autowired
    private NettyServer nettyServer;

    @Autowired
    private AccountParser accountParser;

    @Value("${data.initial.file}")
    private String initFile;

    @Autowired
    private AccountService accountService;

    @Autowired
    private NowProvider nowProvider;

    @Override
    public void run(String... args) throws Exception {
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
        System.out.println("File count: " + accountsFileTreeMap.size());
        Statistics statistics = new Statistics();
        int count = 0;
        byte[] buf = new byte[1000000];
       // for (Integer k = 0; k < 50; k++) {
            for (Map.Entry<Integer, ZipEntry> entry : accountsFileTreeMap.entrySet()) {
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
                           /* accountDTO.id = Integer.valueOf(k.toString() + Integer.toString(accountDTO.id));
                            accountDTO.email = k.toString() + accountDTO.email;
                            if (accountDTO.phone != null) {
                                accountDTO.phone = k.toString() + accountDTO.phone;
                            } */
                            accountService.load(accountDTO);
                            statistics.analyze(accountDTO);
                            count++;
                        }

                    }
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
      //  }
        System.out.println("Finish load " + count + " accounts " + new Date());
        System.out.println(statistics);
        accountService.finishLoad();
        System.out.println("Indexes created");
        new Thread(() -> {
            try {
                nettyServer.start();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

    }
}
