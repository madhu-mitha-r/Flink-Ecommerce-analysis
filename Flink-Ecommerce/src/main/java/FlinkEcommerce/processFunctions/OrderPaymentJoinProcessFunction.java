package FlinkEcommerce.processFunctions;

import FlinkEcommerce.dtos.*;
import FlinkEcommerce.enums.JoinAnomalyType;
import FlinkEcommerce.enums.OrderPaymentJoinStatus;
import FlinkEcommerce.enums.PaymentStatus;
import java.time.Duration;
import java.util.Objects;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OrderPaymentJoinProcessFunction
    extends KeyedCoProcessFunction<String, Order, Payment, PaidOrder> {

  public static final OutputTag<OrderPaymentJoinAnomaly> ANOMALIES =
      new OutputTag<OrderPaymentJoinAnomaly>("join-anomalies") {};

  private static final Duration STATE_TTL = Duration.ofDays(7);
  private static final double AMOUNT_TOLERANCE = 0.01;

  private transient ValueState<OrderPaymentJoinState> orderPaymentJoinState;

  private final long joinTimeoutMs;

  public OrderPaymentJoinProcessFunction(Duration joinTimeout) {
    this.joinTimeoutMs = joinTimeout.toMillis();
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    // assign the state for each subtask
    ValueStateDescriptor<OrderPaymentJoinState> descriptor =
        new ValueStateDescriptor<>("order-payment-join-state", OrderPaymentJoinState.class);
    descriptor.enableTimeToLive(StateTtlConfig.newBuilder(STATE_TTL).build());
    orderPaymentJoinState = getRuntimeContext().getState(descriptor);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<PaidOrder> out)
      throws Exception {
    OrderPaymentJoinState state = orderPaymentJoinState.value();
    if (state == null) {
      return;
    }

    String orderId = ctx.getCurrentKey();
    if (state.getStatus() != null) {
      orderPaymentJoinState.clear();
    } else if (state.getOrder() != null) {
      ctx.output(
          ANOMALIES,
          OrderPaymentJoinAnomaly.of(
              JoinAnomalyType.ABANDONED_ORDER, orderId, state.getOrder(), null));
      long cleanupTs = timestamp + joinTimeoutMs;
      ctx.timerService().registerEventTimeTimer(cleanupTs);
      state.setTimerMs(cleanupTs);
      state.setStatus(OrderPaymentJoinStatus.ABANDONED);
      orderPaymentJoinState.update(state);
    } else {
      ctx.output(
          ANOMALIES,
          OrderPaymentJoinAnomaly.of(
              JoinAnomalyType.ORPHAN_PAYMENT, orderId, null, state.getPayment()));
      orderPaymentJoinState.clear();
    }
  }

  @Override
  public void processElement1(
      Order order,
      KeyedCoProcessFunction<String, Order, Payment, PaidOrder>.Context ctx,
      Collector<PaidOrder> out)
      throws Exception {
    OrderPaymentJoinState state = orderPaymentJoinState.value();
    if (state == null) {
      long timerTs = ctx.timestamp() + joinTimeoutMs;
      state = new OrderPaymentJoinState();
      state.setOrder(order);
      state.setTimerMs(timerTs);
      ctx.timerService().registerEventTimeTimer(timerTs);
    } else if (state.getPayment() != null && state.getStatus() == null) {
      state.setOrder(order);
      completeJoin(state, ctx, out);
    } else {
      return;
    }
    orderPaymentJoinState.update(state);
  }

  private void completeJoin(
      OrderPaymentJoinState state,
      KeyedCoProcessFunction<String, Order, Payment, PaidOrder>.Context ctx,
      Collector<PaidOrder> out) {
    Order order = state.getOrder();
    Payment payment = state.getPayment();

    out.collect(PaidOrder.from(order, payment));
    if (Math.abs(order.getTotalAmount() - payment.getAmount()) > AMOUNT_TOLERANCE) {
      ctx.output(
          ANOMALIES,
          OrderPaymentJoinAnomaly.of(
              JoinAnomalyType.AMOUNT_MISMATCH, order.getOrderId(), order, payment));
    }

    ctx.timerService().deleteEventTimeTimer(state.getTimerMs());
    long cleanupTs = ctx.timestamp() + joinTimeoutMs;
    ctx.timerService().registerEventTimeTimer(cleanupTs);
    state.setTimerMs(cleanupTs);
    state.setStatus(OrderPaymentJoinStatus.COMPLETED);
  }

  @Override
  public void processElement2(
      Payment payment,
      KeyedCoProcessFunction<String, Order, Payment, PaidOrder>.Context ctx,
      Collector<PaidOrder> out)
      throws Exception {
    if (payment.getStatus() != PaymentStatus.SUCCESS) {
      return;
    }

    String orderId = ctx.getCurrentKey();
    OrderPaymentJoinState state = orderPaymentJoinState.value();

    if (state == null) {
      long timerTs = ctx.timestamp() + joinTimeoutMs;
      state = new OrderPaymentJoinState();
      state.setPayment(payment);
      state.setTimerMs(timerTs);
      ctx.timerService().registerEventTimeTimer(timerTs);
    } else if (state.getStatus() == OrderPaymentJoinStatus.ABANDONED) {
      ctx.output(
          ANOMALIES,
          OrderPaymentJoinAnomaly.of(
              JoinAnomalyType.LATE_PAYMENT, orderId, state.getOrder(), payment));
      return;
    } else if (state.getOrder() != null && state.getStatus() == null) {
      state.setPayment(payment);
      completeJoin(state, ctx, out);
    } else if (state.getPayment() != null
        && !Objects.equals(payment.getTransactionId(), state.getPayment().getTransactionId())) {
      ctx.output(
          ANOMALIES,
          OrderPaymentJoinAnomaly.of(
              JoinAnomalyType.DOUBLE_CHARGE, orderId, state.getOrder(), payment));
      return;
    } else {
      return;
    }

    orderPaymentJoinState.update(state);
  }
}
