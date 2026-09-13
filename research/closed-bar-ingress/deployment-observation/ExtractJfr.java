import java.nio.file.*;
import java.io.*;
import java.util.*;
import jdk.jfr.consumer.*;

/** Stream a whitelist of performance events; exclude environment/system-property events. */
public class ExtractJfr {
  static final Set<String> TYPES=Set.of("jdk.ExecutionSample","jdk.ObjectAllocationSample",
      "jdk.FileWrite","jdk.ThreadCPULoad","jdk.CPULoad","jdk.GarbageCollection",
      "jdk.GCPhasePause","jdk.Compilation","jdk.JavaMonitorEnter","jdk.ThreadPark");
  static String quote(String s) {
    if(s==null)return "null";
    StringBuilder b=new StringBuilder("\"");
    for(char c:s.toCharArray()) {
      switch(c) {
        case '"' -> b.append("\\\"");
        case '\\' -> b.append("\\\\");
        case '\n' -> b.append("\\n");
        case '\r' -> b.append("\\r");
        case '\t' -> b.append("\\t");
        default -> {if(c<32)b.append(String.format("\\u%04x",(int)c));else b.append(c);}
      }
    }
    return b.append('"').toString();
  }
  static String json(Object o) {
    if(o==null)return "null";
    if(o instanceof Number || o instanceof Boolean)return o.toString();
    if(o instanceof Map<?,?> m) {
      StringJoiner s=new StringJoiner(",","{","}");
      m.forEach((k,v)->s.add(quote(k.toString())+":"+json(v)));return s.toString();
    }
    if(o instanceof Iterable<?> xs) {
      StringJoiner s=new StringJoiner(",","[","]");for(Object x:xs)s.add(json(x));return s.toString();
    }
    return quote(o.toString());
  }
  public static void main(String[] args)throws Exception {
    long count=0;
    try(RecordingFile in=new RecordingFile(Path.of(args[0]));
        BufferedWriter out=Files.newBufferedWriter(Path.of(args[1]))) {
      while(in.hasMoreEvents()) {
        RecordedEvent e=in.readEvent();String type=e.getEventType().getName();
        if(!TYPES.contains(type))continue;
        Map<String,Object> row=new LinkedHashMap<>();row.put("type",type);
        row.put("at_ms",e.getStartTime().toEpochMilli());row.put("duration_ms",e.getDuration().toNanos()/1e6);
        RecordedThread t=type.equals("jdk.ExecutionSample")?e.getThread("sampledThread"):e.getThread();
        if(t!=null) {row.put("thread",t.getJavaName());row.put("os_tid",t.getOSThreadId());}
        var trace=e.getStackTrace();List<String> stack=new ArrayList<>();
        if(trace!=null)for(RecordedFrame f:trace.getFrames()) {
          if(stack.size()==96)break;
          stack.add(f.getMethod().getType().getName()+"."+f.getMethod().getName());
        }
        if(!stack.isEmpty())row.put("stack",stack);
        switch(type) {
          case "jdk.FileWrite" -> {row.put("path",e.getString("path"));row.put("bytes",e.getLong("bytesWritten"));}
          case "jdk.ObjectAllocationSample" -> {row.put("weight",e.getLong("weight"));row.put("object_class",e.getClass("objectClass").getName());}
          case "jdk.CPULoad" -> {row.put("jvm_user",e.getFloat("jvmUser"));row.put("jvm_system",e.getFloat("jvmSystem"));row.put("machine",e.getFloat("machineTotal"));}
          case "jdk.ThreadCPULoad" -> {row.put("user",e.getFloat("user"));row.put("system",e.getFloat("system"));}
          case "jdk.GarbageCollection" -> {row.put("name",e.getString("name"));row.put("cause",e.getString("cause"));}
          case "jdk.Compilation" -> {row.put("compiler",e.getString("compiler"));row.put("level",e.getInt("compileLevel"));}
          case "jdk.JavaMonitorEnter" -> row.put("monitor_class",e.getClass("monitorClass").getName());
          default -> {}
        }
        out.write(json(row));out.newLine();count++;
      }
    }
    System.out.println("Exported "+count+" whitelisted events");
  }
}
