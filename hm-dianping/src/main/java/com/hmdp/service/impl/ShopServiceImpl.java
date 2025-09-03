package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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

    //tryLock
    private Boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return flag;
    }
    //释放锁
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    //检查redis缓存封装
    private Shop checkShop(Long id){
        //查看redis缓存 有可能返回为空值
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+ id);
        Shop shop = JSONUtil.toBean(shopJson, Shop.class);
        return shop;
    }

    public Result queryById(Long id){
        //设置空值解决缓存穿透
        //Shop shop = queryPassThrough(id);
        Shop shop = queryWithMutex2(id);
        if(shop == null){
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);
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
        //如果为空值则获取互斥锁
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


    private Shop queryWithMutex(Long id){
        //查看redis缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+ id);
        //是否存在缓存，存在直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断命中是否为空值 由于isNotBlank过滤出“”和null 不等于null就为“”
        //如果为空值则返回空
        if (shopJson != null) {
            return null;
        }
        //不存在缓存则重建缓存 获取互斥锁
        String lockKey = null;
        Shop shop = null;
        try {
            lockKey = LOCK_SHOP_KEY + id;
            Boolean isLock = tryLock(lockKey);
            if (!isLock){
                //没获取锁则睡会儿然后重试
                Thread.sleep(50);
                queryWithMutex(id);
            }
            // 获取锁后二次检查redis是否存在缓存 不存在则重建缓存
            Shop checkShop = checkShop(id);
            if (checkShop != null) {//存在则直接返回缓存
                return checkShop;
            }
            //重建缓存
            shop = getById(id);
            if (shop == null) {
                //将空值缓存到redis
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "",2L, TimeUnit.MINUTES);
                return null;
            }
            //存入缓存，并设置过期时间
            String jsonStr = JSONUtil.toJsonStr(shop);
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, jsonStr,30L, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            unlock(lockKey);
        }
        return shop;
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
