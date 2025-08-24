package com.hmdp.service.impl;

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

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

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

    @Override
    public Result queryById(Long id) {
        //查看redis缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+ id);
        //是否存在缓存，存在直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }
        //不存在查数据库
        Shop shop = getById(id);
        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        //存入缓存，并设置过期时间
        String jsonStr = JSONUtil.toJsonStr(shop);
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, jsonStr,30L, TimeUnit.MINUTES);

        return Result.ok(shop);
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
