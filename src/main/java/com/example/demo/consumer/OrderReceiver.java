package com.example.demo.consumer;

import com.example.demo.entity.Order;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author laowang
 * @date 2018/11/6 6:45 PM
 * @Description:
 */
@Component
public class OrderReceiver {

    /**
     * 注解的方式监听
     */
    @RabbitListener(bindings = @QueueBinding(
                    value = @Queue(value = "order-queue",durable = "true"),
                    exchange = @Exchange(value = "order-exchange",durable = "true",type = "topic"),
                    key = "order.*"
    ))
    @RabbitHandler
    public void onOrderMessage(
            @Payload Order order,   // 消息体内容
            Channel channel,        // 通道
            @Headers Map<String,Object> headers // 消息头
            ) throws Exception{

        // 消费者操作
        System.out.println("收到消息，开始消费");
        System.out.println("订单ID: " + order.getId());

        Long deliveryTay = (Long)headers.get(AmqpHeaders.DELIVERY_TAG);

        // ACK 手工调用 queues 才能消费
        channel.basicAck(deliveryTay,false);
    }
}
