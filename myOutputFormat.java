import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class myOutputFormat<K, V>
  extends TextOutputFormat<K, V>
{
  private FileOutputCommitter myCommitter = null;
  
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException
  {
    if (this.myCommitter == null)
    {
      Path output = new Path(getOutputDir(context));
      this.myCommitter = new FileOutputCommitter(output, context);
    }
    return this.myCommitter;
  }
  
  protected static String getOutputDir(TaskAttemptContext context)
  {
    int taskID = context.getTaskAttemptID().getTaskID().getId();
    String taskType = context.getTaskAttemptID().getTaskID().getTaskType().toString();
    System.err.println("MyDebug: taskattempt id is: " + taskID + " and tasktype is: " + taskType);
    String outputBaseDir = getOutputPath(context).toString() + "/mysubdir"; 
    return outputBaseDir;
  }
  
}
