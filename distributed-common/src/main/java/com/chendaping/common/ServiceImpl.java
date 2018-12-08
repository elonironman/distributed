package com.chendaping.common;

/**
 * Created by chendaping on 2018/12/8.
 */
public class ServiceImpl implements IService {
    public String hello(String name) {
        return "hello  " + name;
    }
}
