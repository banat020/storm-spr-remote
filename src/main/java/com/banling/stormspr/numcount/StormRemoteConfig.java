package com.banling.stormspr.numcount;

import org.apache.storm.Config;

public class StormRemoteConfig {
	
	public static Config config() {
		Config config = new Config();
        config.setDebug(false);
        return config;
	}

}
