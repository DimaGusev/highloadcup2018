package com.dgusev.hlcup2018.accountsapp.init;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import com.dgusev.hlcup2018.accountsapp.parse.AccountParser;
import com.dgusev.hlcup2018.accountsapp.service.AccountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@Component
public class DataLoader implements CommandLineRunner {

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
                } else if (zipEntry.getName().equals("options.txt")) {
                    int now = new Scanner(zipFile.getInputStream(zipEntry)).nextInt();
                    nowProvider.initNow(now);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }

        });
        System.out.println("Start load data" + new Date());
        System.out.println("File count: " + accountsFileTreeMap.size());
        Statistics statistics = new Statistics();
        int count = 0;
        byte[] buf = new byte[1000000];
        int cnt = 0;
            for (Map.Entry<Integer, ZipEntry> entry : accountsFileTreeMap.entrySet()) {
                cnt++;
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
                            accountService.load(accountDTO);
                            statistics.analyze(accountDTO);
                            count++;
                        }

                    }
                    inputStream.close();
                    System.out.println("Finish load data " + new Date());
                    if (cnt%10 ==0) {
                        System.out.println(statistics);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        System.out.println("Finish load " + count + " accounts " + new Date());
        System.out.println(statistics);

    }

    private byte[] readFile(InputStream inputStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        StringBuilder stringBuilder = new StringBuilder();
        String line = null;
        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
        }
        return stringBuilder.toString().getBytes();
    }
}
