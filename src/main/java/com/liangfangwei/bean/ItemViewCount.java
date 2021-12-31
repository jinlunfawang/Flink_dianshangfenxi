package com.liangfangwei.bean;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: UserBehaviorAnalysis
 * Package: com.atguigu.hotitems_analysis.beans
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/14 15:13
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor

public class ItemViewCount {
    private Long itemId;
    private Long windowEnd;
    private Long count;


}
