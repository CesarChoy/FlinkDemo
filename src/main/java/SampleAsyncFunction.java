import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.eclipse.jetty.util.Callback;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class SampleAsyncFunction extends RichAsyncFunction<Integer, String> {

    private long[] sleep = {100L, 1000L, 5000L, 2000L, 600L, 100L};

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void asyncInvoke(Integer input, ResultFuture<String> resultFuture) {
        //打印时间
        System.out.println("当前时间：" + System.currentTimeMillis() + " ，异步处理：" + input + " ，停止时间：" + sleep[input] + "ms");

        //query(input, resultFuture);
        asyncQuery(input, resultFuture);
    }

    /**
     * 会阻塞查询相当于同步IO
     *
     * @param input
     * @param resultFuture
     */
    private void query(Integer input, final ResultFuture<String> resultFuture) {
        try {
            Thread.sleep(sleep[input]);
            //通过ResultFuture的对象 调用complete达到发送数据到下游的效果 complete方法可以简单类似成collector的collect方法
            resultFuture.complete(Collections.singletonList(String.valueOf(input)));
        } catch (InterruptedException e) {
            e.printStackTrace();
            resultFuture.complete(new ArrayList<String>(0));
        }
    }

    /**
     * 这个方法并不是异步的，而是要依靠这个方法里面所写的查询是异步的才可以
     * <p>
     * 使用Async I/O，需要外部存储有支持异步请求的客户端
     * 使用Async I/O，继承RichAsyncFunction(接口AsyncFunction<IN, OUT>的抽象类)，重写或实现open(建立连接)、close(关闭连接)、asyncInvoke(异步调用)3个方法即可。
     * 使用Async I/O, 最好结合缓存一起使用，可减少请求外部存储的次数，提高效率。
     * Async I/O 提供了Timeout参数来控制请求最长等待时间。默认，异步I/O请求超时时，会引发异常并重启或停止作业。 如果要处理超时，可以重写AsyncFunction#timeout方法。
     * Async I/O 提供了Capacity参数控制请求并发数，一旦Capacity被耗尽，会触发反压机制来抑制上游数据的摄入。
     * Async I/O 输出提供乱序和顺序两种模式。
     *
     * @param input
     * @param resultFuture
     */
    private void asyncQuery(final Integer input, final ResultFuture<String> resultFuture) {
//        // 核心！！！
//        Callback.Completable.supplyAsync(new Supplier<Integer>() {
//            @Override
//            public Integer get() {
//                try {
//                    Thread.sleep(sleep[input]);
//                    return input;
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                    return null;
//                }
//            }
//            // 批量查询
//        }).thenAccept((Integer dbResult) -> {
//            // 查询结果是异步返回
//            resultFuture.complete(Collections.singleton(String.valueOf(dbResult)));
//        });

        CompletableFuture.supplyAsync(new Supplier<Integer>() {

            @Override
            public Integer get() {
                try {
                    Thread.sleep(sleep[input]);
                    return input;
                } catch (Exception e) {
                    return null;
                }
            }
        }).thenAccept((Integer dbResult) -> {
            resultFuture.complete(Collections.singleton(String.valueOf(dbResult)));
        });

    }

}