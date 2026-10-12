package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {
        // 方案一：缓存穿透（空值缓存）
        // Shop shop = queryWithPassThrough(id);

        // 方案二：互斥锁解决缓存击穿（在空值缓存的基础上，用锁保证同一时刻只有一个线程重建缓存）
        Shop shop = queryWithMutex(id);

        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);
    }

    /**
     * 缓存穿透解决方案：空值缓存
     * <p>查不到的数据也写进 Redis（空字符串 + 短 TTL），避免同一个不存在的 id 反复打到数据库</p>
     */
    private Shop queryWithPassThrough(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1. 从 Redis 查询商铺缓存（未命中是 null，命中空值是 ""）
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2. 命中真实数据 → 直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 3. 命中空值（即 shopJson != null 但内容为空串）→ 数据库里也没有，直接返回 null
        if (shopJson != null) {
            return null;
        }
        // 4. 未命中 → 查数据库
        Shop shop = getById(id);
        // 5. 数据库也没有 → 写入空值缓存（短 TTL），返回 null
        if (shop == null) {
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 6. 数据库有 → 写入缓存（TTL = 基础 30 分钟 + 随机 0~9 分钟，防缓存雪崩）
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),
                RedisConstants.CACHE_SHOP_TTL + RandomUtil.randomLong(0, 10), TimeUnit.MINUTES);
        // 7. 返回
        return shop;
    }

    /**
     * 缓存击穿解决方案：互斥锁（分布式锁）
     * <p>热点 key 失效瞬间，只让抢到锁的一个线程去查库并重建缓存，其余线程短暂休眠后重试（重试时通常已能读到缓存）</p>
     */
    private Shop queryWithMutex(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1. 从 Redis 查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2. 命中真实数据 → 直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 3. 命中空值 → 数据确实不存在，直接返回 null
        if (shopJson != null) {
            return null;
        }

        // 4. 未命中 → 准备重建缓存，先尝试获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 4.1 没有抢到锁：说明已有线程在重建，休眠一会儿后重试（重试会先读缓存）
        if (!isLock) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            // 递归重试；生产环境更推荐"循环 + 最大重试次数"，避免极端并发下递归过深
            return queryWithMutex(id);
        }

        // 4.2 抢到锁：查库 + 重建缓存；无论成功失败都要释放锁
        Shop shop;
        try {
            // 双重检查：抢锁期间可能已有其他线程把缓存重建好了
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(shopJson)) {
                return JSONUtil.toBean(shopJson, Shop.class);
            }
            if (shopJson != null) {
                return null;
            }

            // 查数据库
            shop = getById(id);
            // 数据库也没有 → 写空值缓存（穿透兜底），返回 null
            if (shop == null) {
                stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 数据库有 → 写缓存（TTL 加随机抖动，防雪崩）
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),
                    RedisConstants.CACHE_SHOP_TTL + RandomUtil.randomLong(0, 10), TimeUnit.MINUTES);
        } finally {
            // 4.3 释放互斥锁：只有抢到锁的线程才走到这里，避免误删别人的锁
            unLock(lockKey);
        }
        // 5. 返回
        return shop;
    }

    // 获取互斥锁：SET key value NX EX —— "加锁 + 设过期时间"是一条原子命令
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1",
                RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    // 释放互斥锁
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }

    // 缓存更新策略：先更新数据库，再删除缓存
    @Override
    @Transactional
    public Result update(Shop shop) {

        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺ID不能为空!");
        }

        // 1.修改数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
