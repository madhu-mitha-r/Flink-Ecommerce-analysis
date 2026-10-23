/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package FlinkEcommerce;

import FlinkEcommerce.deserializers.GeneralJsonDeserializer;
import FlinkEcommerce.dtos.*;
import FlinkEcommerce.processFunctions.OrderPaymentJoinProcessFunction;
import FlinkEcommerce.processFunctions.ProductEnrichmentFunction;
import FlinkEcommerce.processFunctions.RevenuePerCategoryAggregateFunction;
import FlinkEcommerce.processFunctions.RevenuePerCategoryWindowProcessFunction;
import java.sql.Timestamp;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

public class DataStreamJob {

  public static void main(String[] args) throws Exception {

    Configuration config = new Configuration();
    config.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
    config.set(
        CheckpointingOptions.CHECKPOINTS_DIRECTORY,
        "file:///Users/madhumitharaju/tools/flink-checkpoints/eco");
    config.set(
        CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(3);

    final String ordersTopic = "orders";
    final String paymentsTopic = "payments";

    final String UPSERT_REVENUE_PER_CATEGORY =
        "INSERT INTO revenue_per_category (window_start, window_end, category, units_sold, revenue) "
            + "VALUES (?, ?, ?, ?, ?) "
            + "ON CONFLICT (window_start, category) DO UPDATE SET "
            + "window_end = EXCLUDED.window_end, "
            + "units_sold = EXCLUDED.units_sold, "
            + "revenue = EXCLUDED.revenue";

    KafkaSource<Order> orderKafkaSource =
        KafkaSource.<Order>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics(ordersTopic)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new GeneralJsonDeserializer<>(Order.class))
            .setGroupId("flink-order-stream")
            .build();

    KafkaSource<Payment> paymentKafkaSource =
        KafkaSource.<Payment>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics(paymentsTopic)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new GeneralJsonDeserializer<>(Payment.class))
            .setGroupId("flink-payment-stream")
            .build();

    KafkaSource<ProductCdcEvent> productCdcEventKafkaSource =
        KafkaSource.<ProductCdcEvent>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("product_cdc")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new GeneralJsonDeserializer<>(ProductCdcEvent.class))
            .setGroupId("flink-product-cdc-stream")
            .build();

    DataStream<Order> orderDataStream =
        env.fromSource(
            orderKafkaSource,
            WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(
                    (element, recordTimestamp) ->
                        element.getEventTime() == null ? recordTimestamp : element.getEventTime())
                .withIdleness(Duration.ofSeconds(10)),
            "order_events");

    DataStream<Payment> paymentDataStream =
        env.fromSource(
            paymentKafkaSource,
            WatermarkStrategy.<Payment>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(
                    (element, recordTimestamp) ->
                        element.getEventTime() == null ? recordTimestamp : element.getEventTime())
                .withIdleness(Duration.ofSeconds(10)),
            "payment_events");

    DataStream<ProductCdcEvent> productCdcEventDataStream =
        env.fromSource(
            productCdcEventKafkaSource,
            WatermarkStrategy.<ProductCdcEvent>forMonotonousTimestamps()
                .withTimestampAssigner(
                    (element, recordTimestamp) ->
                        element.getTsMs() == null ? recordTimestamp : element.getTsMs())
                .withIdleness(Duration.ofSeconds(10)),
            "product_cdc_events");

    SingleOutputStreamOperator<PaidOrder> paidOrderStream =
        orderDataStream
            .keyBy(Order::getOrderId)
            .connect(paymentDataStream.keyBy(Payment::getOrderId))
            .process(new OrderPaymentJoinProcessFunction())
            .uid("order-payment-join");

    //    paidOrderStream.print("PAID");
    //
    // paidOrderStream.getSideOutput(OrderPaymentJoinProcessFunction.ABANDONED).print("ABANDONED");

    BroadcastStream<ProductCdcEvent> productCdcEventBroadcastStream =
        productCdcEventDataStream.broadcast(ProductEnrichmentFunction.PRODUCTS);

    SingleOutputStreamOperator<SoldItem> soldItemSingleOutputStream =
        paidOrderStream
            .connect(productCdcEventBroadcastStream)
            .process(new ProductEnrichmentFunction())
            .uid("product-enrichment");

    //    soldItemSingleOutputStream.print("SoldItem");

    SingleOutputStreamOperator<RevenuePerCategory> revenuePerCategoryDataStream =
        soldItemSingleOutputStream
            .keyBy(SoldItem::getProductCategory)
            .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
            .aggregate(
                new RevenuePerCategoryAggregateFunction(),
                new RevenuePerCategoryWindowProcessFunction())
            .uid("revenue-per-category");

    revenuePerCategoryDataStream.print("RevenuePerCategory");

    revenuePerCategoryDataStream
        .sinkTo(
            JdbcSink.<RevenuePerCategory>builder()
                .withQueryStatement(
                    UPSERT_REVENUE_PER_CATEGORY,
                    (preparedStatement, revenuePerCategory) -> {
                      preparedStatement.setTimestamp(
                          1, new Timestamp(revenuePerCategory.getWindowStartTime()));
                      preparedStatement.setTimestamp(
                          2, new Timestamp(revenuePerCategory.getWindowEndTime()));
                      preparedStatement.setString(3, revenuePerCategory.getProductCategory());
                      preparedStatement.setLong(4, revenuePerCategory.getUnitsSold());
                      preparedStatement.setBigDecimal(5, revenuePerCategory.getTotalRevenue());
                    })
                .withExecutionOptions(
                    JdbcExecutionOptions.builder()
                        .withBatchSize(500)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(2)
                        .build())
                .buildAtLeastOnce(
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:postgresql://localhost:5432/ecommerce")
                        .withDriverName("org.postgresql.Driver")
                        .withUsername("admin")
                        .withPassword("shop")
                        .build()))
        .uid("sink-revenue-per-category-db");

    env.execute("Flink E-commerce Analysis");
  }
}
