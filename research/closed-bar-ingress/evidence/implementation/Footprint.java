import java.lang.instrument.Instrumentation;
public class Footprint {
  static Instrumentation instrumentation;
  public static void premain(String args, Instrumentation value) { instrumentation = value; }
  record BoxedVersion(Long eventTime, long sequence) { }
  record PrimitiveVersion(long eventTime, long sequence, boolean known) { }
  public static void main(String[] args) {
    Long time = Long.valueOf(1789315200109L);
    System.out.println("{\"boxed_record\":" + instrumentation.getObjectSize(new BoxedVersion(time, 1))
      + ",\"boxed_timestamp\":" + instrumentation.getObjectSize(time)
      + ",\"primitive_record\":" + instrumentation.getObjectSize(new PrimitiveVersion(time, 1, true)) + "}");
  }
}
