package com.dgusev.hlcup2018.accountsapp;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import com.dgusev.hlcup2018.accountsapp.parse.AccountParser;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class AccountsAppApplicationTests {

	@Autowired
	private AccountParser accountParser;

	@Test
	public void contextLoads() {
		AccountDTO accountDTO = accountParser.parse("{\"id\" : 123, \"interests\": [\"\\u0410\\u0432\\u0442\\u043e\\u043c\\u043e\\u0431\\u0438\\u043b\\u0438\", \"\\u0420\\u0435\\u0433\\u0433\\u0438\", \"\\u0417\\u043d\\u0430\\u043a\\u043e\\u043c\\u0441\\u0442\\u0432\\u043e\"], \"email\": \"qwrqwr@gmail.com\", \"premium\": {\"start\": 1534782167, \"finish\": 1566318167}, \"likes\": [{\"id\": 6601, \"ts\": 1489227074}, {\"id\": 7741, \"ts\": 1524402783}, {\"id\": 6871, \"ts\": 1509247587}, {\"id\": 7463, \"ts\": 1522838923}, {\"id\": 6547, \"ts\": 1505703615}, {\"id\": 1267, \"ts\": 1494054554}, {\"id\": 6947, \"ts\": 1467509383}, {\"id\": 7767, \"ts\": 1538601358}, {\"id\": 9539, \"ts\": 1463540503}, {\"id\": 8907, \"ts\": 1540636217}, {\"id\": 9195, \"ts\": 1477935027}, {\"id\": 1305, \"ts\": 1531678222}, {\"id\": 1951, \"ts\": 1454555372}, {\"id\": 765, \"ts\": 1474608579}, {\"id\": 2611, \"ts\": 1512945502}]}".getBytes());
	}




}

