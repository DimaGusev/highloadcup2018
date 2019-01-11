package com.dgusev.hlcup2018.accountsapp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;
import java.util.Objects;

@SpringBootApplication
@ComponentScan({"com.dgusev.hlcup2018.accountsapp","io.netty.channel.epoll"})
public class AccountsAppApplication {

	public static void main(String[] args) {
		SpringApplication.run(AccountsAppApplication.class, args);
	}

}

