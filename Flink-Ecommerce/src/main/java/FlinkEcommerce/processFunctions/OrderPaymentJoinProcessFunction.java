package FlinkEcommerce.processFunctions;

import FlinkEcommerce.dtos.Order;
import FlinkEcommerce.dtos.PaidOrder;
import FlinkEcommerce.dtos.Payment;
import FlinkEcommerce.enums.PaymentStatus;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OrderPaymentJoinProcessFunction
    extends KeyedCoProcessFunction<String, Order, Payment, PaidOrder> {

  public static final OutputTag<Order> ABANDONED = new OutputTag<Order>("abandoned-orders") {};

  private static final long PAYMENT_TIMEOUT_MS = 60_000;

  private transient ValueState<Order> orderState;

  @Override
  public void open(OpenContext openContext) throws Exception {
    // assign the state for each subtask
    orderState = getRuntimeContext().getState(new ValueStateDescriptor<>("order", Order.class));
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<PaidOrder> out)
      throws Exception {
    Order order = orderState.value();
    if (order != null) {
      ctx.output(ABANDONED, order);
      orderState.clear();
    }
  }

  @Override
  public void processElement1(
      Order order,
      KeyedCoProcessFunction<String, Order, Payment, PaidOrder>.Context ctx,
      Collector<PaidOrder> out)
      throws Exception {
    // first we get order and then payment , store the order in state, until we get a successful
    // payment
    orderState.update(order);
    ctx.timerService().registerEventTimeTimer(order.getEventTime() + PAYMENT_TIMEOUT_MS);
  }

  @Override
  public void processElement2(
      Payment payment,
      KeyedCoProcessFunction<String, Order, Payment, PaidOrder>.Context ctx,
      Collector<PaidOrder> out)
      throws Exception {
    Order order = orderState.value();
    if (order == null || payment.getStatus() != PaymentStatus.SUCCESS) {
      return;
    }
    out.collect(PaidOrder.from(order, payment));
    ctx.timerService().deleteEventTimeTimer(order.getEventTime() + PAYMENT_TIMEOUT_MS);
    orderState.clear();
  }
}
