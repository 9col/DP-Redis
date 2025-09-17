package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;



    //检查redis缓存封装
    private Shop checkShop(Long id){
        //查看redis缓存 有可能返回为空值
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+ id);
        Shop shop = JSONUtil.toBean(shopJson, Shop.class);
        return shop;
    }


    private Shop queryPassThrough(Long id){
        //查看redis缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+ id);
        //是否存在缓存，存在直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断命中是否为空值 由于isNotBlank过滤掉了“”和null 不等于null就为“”
        if (shopJson != null) {
            return null;
        }
        //不存在查数据库
        Shop shop = getById(id);
        if (shop == null) {
            //将空值缓存到redis
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "",2L, TimeUnit.MINUTES);
            return null;
        }
        //存入缓存，并设置过期时间
        String jsonStr = JSONUtil.toJsonStr(shop);
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, jsonStr,30L, TimeUnit.MINUTES);
        return shop;
    }

    //tryLock
    private Boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return flag;
    }
    //释放锁
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
    /**
     * 重构版  同时解决缓存穿透 缓存击穿 缓存雪崩
     * 使用while循环避免递归造成栈溢出问题
     * @param id
     * @return
     */
    private Shop queryWithMutex2(Long id) {
        String key = CACHE_SHOP_KEY + id;
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;

        while (true) {
            // 1. 查询缓存
            String shopJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(shopJson)) {
                return JSONUtil.toBean(shopJson, Shop.class);
            }
            // 命中空值缓存
            if (shopJson != null) {
                return null;
            }

            // 2. 尝试获取互斥锁
            boolean isLock = tryLock(lockKey);
            if (!isLock) {
                // 未获取到锁，稍等后重试
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    // 恢复中断状态，避免线程被吞掉
                    Thread.currentThread().interrupt();
                    return null;
                }
                continue;
            }

            try {
                // 3. 获取到锁 -> 再次检查缓存（避免重复构建）
                String cacheAgain = stringRedisTemplate.opsForValue().get(key);
                if (StrUtil.isNotBlank(cacheAgain)) {
                    return JSONUtil.toBean(cacheAgain, Shop.class);
                }
                if (cacheAgain != null) {
                    return null;
                }
                //模拟重建的延时
                Thread.sleep(200);
                // 4. 查询数据库
                shop = getById(id);
                if (shop == null) {
                    // 缓存空值，防止穿透
                    stringRedisTemplate.opsForValue().set(key, "", 2, TimeUnit.MINUTES);
                    return null;
                }

                // 5. 写入缓存（加随机过期时间，防止雪崩）
                stringRedisTemplate.opsForValue().set(
                        key,
                        JSONUtil.toJsonStr(shop),
                        30 + ThreadLocalRandom.current().nextInt(10), // 30~40分钟
                        TimeUnit.MINUTES
                );
                return shop;
            }catch (Exception e){
                throw new RuntimeException(e);
            }
            finally {
                // 6. 释放锁（只在成功获取锁时释放）
                unlock(lockKey);
            }
        }
    }

    public Result queryById(Long id){
        //设置空值解决缓存穿透
        //Shop shop = queryPassThrough(id);

        //互斥锁解决缓存击穿
        //Shop shop = queryWithMutex2(id);

        //逻辑过期解决缓存击穿
        Shop shop = queryWithLogicalExpire(id);
        if(shop == null){
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);
    }

    private void saveShop2Redis (Long Id,Long expireSeconds) throws InterruptedException {
        //热点key问题一般会事先把热点key放入redis，所有不会出现查不到的情况
        Shop shop = getById(Id);

        //sleep模拟缓存重建
        Thread.sleep(200);
        //封装逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+ Id, JSONUtil.toJsonStr(redisData));
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    private Shop queryWithLogicalExpire(Long id) {
        //查看redis缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //是否存在缓存，不存在直接返回空
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }

        //将Json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        //存在则判断逻辑时间是否过期
        LocalDateTime expireTime = redisData.getExpireTime();
        //未过期直接返回
        if (expireTime.isAfter(LocalDateTime.now())){
            return shop;
        }
        String lockKey = LOCK_SHOP_KEY + id;
        //（过期了）先获取互斥锁，未获取到返回旧数据
        if ( !tryLock(lockKey) ) {
            return shop;
        }
        //二次检查缓存是否过期
        if (expireTime.isAfter(LocalDateTime.now())){
            return shop;
        }
        //开启独立线程做缓存重建
        CACHE_REBUILD_EXECUTOR.submit(() ->{
            try {
                this.saveShop2Redis(id,30L);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }finally {
                unlock(lockKey);
            }
        });
        return shop;
    }


    @Override
    @Transactional
    public Result update(Shop shop) {
        if (shop.getId()==null) {
            return Result.fail("店铺id不能为空");
        }
        //先更新数据库
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        //延迟删除
        return Result.ok();
    }
}
