package com.billion.worker.fair;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author Billion
 * @create 2021/03/28 15:57
 */
public class Sender {
    public static void main(String[] args) {
        new Thread(new Fanout()).start();
    }

    static class Fanout implements Runnable {
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
                //指定默认交换机信息
                String exName = "";
                String type = "direct";
                //指定队列信息
                for (int i = 0; i < 20; i++) {
                    //发送消息
                    String msg = "worker模式：hello world " + i;
                    channel.basicPublish(exName, "queue7", null, msg.getBytes());
                }
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
}
