package com.hmdp;

import com.hmdp.pojo.redisDemo;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private CacheClient cacheClient;

//    private redisDemo redisdemo;

    @Test
    public void redSet() {
        redisDemo redisdemo = new redisDemo();
        redisdemo.setId("redSet");
        redisdemo.setData("将任意Java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间");
        String key = RedisConstants.DEMO_KEY + redisdemo.getId();
        Long time = 30L;
        TimeUnit unit = TimeUnit.MINUTES;
        cacheClient.set(key, redisdemo, time, unit);
    }


}
