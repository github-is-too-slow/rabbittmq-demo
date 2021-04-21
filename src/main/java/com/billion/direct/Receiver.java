package com.billion.direct;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author Billion
 * @create 2021/03/28 15:57
 */
public class Receiver {
    public static void main(String[] args) {
        new Thread(new Fanout(), "queue4").start();
        new Thread(new Fanout(), "queue5").start();
        new Thread(new Fanout(), "queue6").start();
    }

    static class Fanout implements Runnable{
        public void run() {
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
                //接受消息
                String queueName = Thread.currentThread().getName();
                channel.basicConsume(queueName, true, new DeliverCallback() {
                    public void handle(String consumerTag, Delivery message) throws IOException {
                        System.out.println("msg = " + new String(message.getBody(), "utf-8"));
                        System.out.println("接受成功");
                    }
                }, new CancelCallback() {
                    public void handle(String consumerTag) throws IOException {
                        System.out.println("接受消息失败");
                    }
                });
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
}
