package FlinkEcommerce.processFunctions;

import FlinkEcommerce.dtos.*;
import java.util.List;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class ProductEnrichmentFunction
    extends BroadcastProcessFunction<PaidOrder, ProductCdcEvent, SoldItem> {

  public static final MapStateDescriptor<String, Product> PRODUCTS =
      new MapStateDescriptor<String, Product>("products", String.class, Product.class);

  @Override
  public void processElement(
      PaidOrder paidOrder,
      BroadcastProcessFunction<PaidOrder, ProductCdcEvent, SoldItem>.ReadOnlyContext ctx,
      Collector<SoldItem> out)
      throws Exception {
    List<OrderItem> items = paidOrder.getItems();
    ReadOnlyBroadcastState<String, Product> products = ctx.getBroadcastState(PRODUCTS);

    for (OrderItem orderItem : items) {

      Product product = products.get(orderItem.getProductId());
      if (product == null) {
        continue;
        // if we want to wait for the product to be available in the broadcast state , then it
        // should be keyedBroadcast function with product id as the key
      }

      SoldItem soldItem = new SoldItem();

      soldItem.setProductId(orderItem.getProductId());
      soldItem.setQuantity(orderItem.getQuantity());
      soldItem.setUnitPrice(orderItem.getUnitPrice());

      soldItem.setOrderId(paidOrder.getOrderId());
      soldItem.setCustomerId(paidOrder.getCustomerId());
      soldItem.setPaidAt(paidOrder.getPaidAt());

      soldItem.setProductCategory(product.getCategory());
      soldItem.setProductBrand(product.getBrand());
      soldItem.setProductName(product.getProductName());
      soldItem.setProductPrice(product.getUnitPrice());
      soldItem.setTotalPrice(orderItem.getUnitPrice() * orderItem.getQuantity());

      out.collect(soldItem);
    }
  }

  @Override
  public void processBroadcastElement(
      ProductCdcEvent productCdcEvent,
      BroadcastProcessFunction<PaidOrder, ProductCdcEvent, SoldItem>.Context ctx,
      Collector<SoldItem> out)
      throws Exception {

    if (productCdcEvent.getOp() == null) {
      return;
    }

    BroadcastState<String, Product> broadcastState = ctx.getBroadcastState(PRODUCTS);

    switch (productCdcEvent.getOp()) {
      case CREATE, UPDATE, READ -> {
        if (productCdcEvent.getAfter() != null) {
          broadcastState.put(productCdcEvent.getAfter().getProductId(), productCdcEvent.getAfter());
        }
      }
      case DELETE -> {
        if (productCdcEvent.getBefore() != null) {
          broadcastState.remove(productCdcEvent.getBefore().getProductId());
        }
      }
    }
  }
}
