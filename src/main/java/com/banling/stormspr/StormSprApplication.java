package com.banling.stormspr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootApplication
@Import(com.banling.stormspr.config.SpringContext.class)
public class StormSprApplication {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(StormSprApplication.class);
	
	private static AtomicBoolean flag=new AtomicBoolean(false);

	public synchronized  static void run(String... args) {
		if(flag.compareAndSet(false, true)) {
			LOGGER.info("SpringBoot is starting");
			
			SpringApplication springApplication = new SpringApplication(StormSprApplication.class);
	        //忽略Spring启动信息日志
	        springApplication.setLogStartupInfo(false);
	        springApplication.run(args);

			LOGGER.info("SpringBoot launched");
		}
	}

}
