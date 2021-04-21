package com.billion.simple;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author Billion
 * @create 2021/03/28 15:57
 */
public class Sender {
    public static void main(String[] args) {
        //创建连接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("39.103.174.115");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin");
        connectionFactory.setVirtualHost("/");
        Connection conn = null;
        Channel channel = null;
        try {
            //创建连接
            conn = connectionFactory.newConnection("新连接生产者");
            //创建通道
            channel = conn.createChannel();
            //声明队列
            String queueName = "queue2";
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("name", "jack");
            channel.queueDeclare(queueName, true, false, false, map);
            //发送消息
            String msg = "hello world";
            channel.basicPublish("", queueName, null, msg.getBytes());
            System.out.println("发送成功");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //关闭通道
            if(channel != null && channel.isOpen()){
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
            }
            //关闭连接
            if(conn != null && conn.isOpen()){
                try {
                    conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
