package com.bigdata.converter;


import com.bigdata.kv.base.BaseDimension;

public interface DimensionConverter {
    int getDimensionID(BaseDimension dimension);
}
