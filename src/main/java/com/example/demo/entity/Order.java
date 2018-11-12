package com.example.demo.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * @author laowang
 * @date 2018/11/6 4:30 PM
 * @Description:
 */
@Data
public class Order implements Serializable {

    private static final long serialVersionUID = -4544831095316866375L;
    private String id;
    private String name;

    /**
     * 存储消息发送的唯一标示
     */
    private String messageId;
}
