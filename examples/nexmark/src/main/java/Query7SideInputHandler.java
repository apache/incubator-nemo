import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.runtime.lambda.LambdaSideInputHandler;

public final class Query7SideInputHandler implements
  LambdaSideInputHandler<WindowedValue<Bid>, WindowedValue<Long>, WindowedValue<Bid>> {

  @Override
  public void processMainAndSideInput(WindowedValue<Bid> mainInput, WindowedValue<Long> sideInput,
                                               OutputCollector<WindowedValue<Bid>> collector) {
    if (mainInput.getValue().price == sideInput.getValue()) {
      collector.emit(mainInput);
    }
  }
}
