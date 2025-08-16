package com.hmdp.service.impl;

import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private static final String CACHE_SHOP_TYPE_KEY ="cache:shopType:";
    private static final long BASE_EXPIRATION_TIME =60;

    @Override
    public List<ShopType> queryByType() {
        //检查redis缓存是否存在
        String shopType = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE_KEY);
        if (StrUtil.isNotBlank(shopType)) {
            return JSONUtil.toList(shopType, ShopType.class);
        }
        //不存在查询数据库并存入redis
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();

        //给缓存过期时间增加一些随机性
        long randomExpirationTime = (long) (Math.random() * 10);
        long totalTime = randomExpirationTime + BASE_EXPIRATION_TIME;
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_TYPE_KEY, JSONUtil.toJsonStr(shopTypeList),totalTime, TimeUnit.MINUTES);

        return shopTypeList;
    }
}
