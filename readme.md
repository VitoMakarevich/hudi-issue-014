# Issue & reproduction

If we have the file written without overrides for
`spark.hadoop.parquet.avro.write-old-list-structure` - we receive 2 level structure(from `parquet meta`):
```
  required group internal_list (LIST) {
    repeated int64 array;
  }
```

When later the file receives updates - but this time with
`"spark.hadoop.parquet.avro.write-old-list-structure" -> "false"`
set explicitly in the Spark Session - it fails with the [following error](#stacktrace-of-error-hudi-0141).

But - this happens only on Hudi 0.14.1(did not check when exactly it starts to, e.g. 0.13.x or 0.14.x).
When I run exactly same code on Hudi 0.12.1 - it works fine and able to produce 3 level structure.
e.g. 0.12.1 new file structure:
```
   required group internal_list (LIST) {
    repeated group list {
      required int64 element;
    }
  }
```
You can change the version in `build.sbt` to check it.

## Potential solution
If I completely remove `spark.hadoop.parquet.avro.write-old-list-structure` from Spark Session,
0.14.1 manages to proceed, but writes 2 level structure, which as I understand will not be able to
handle case when there will be `null`s in arrays.


## Stacktrace of error Hudi 0.14.1
```
Driver stacktrace:
	at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2785)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2721)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:2720)
	at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
	at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2720)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1206)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1206)
	at scala.Option.foreach(Option.scala:407)
	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1206)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:2984)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2923)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2912)
	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
	at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:971)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2263)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2284)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2303)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2328)
	at org.apache.spark.rdd.RDD.count(RDD.scala:1266)
	at org.apache.hudi.HoodieSparkSqlWriterInternal.commitAndPerformPostOperations(HoodieSparkSqlWriter.scala:1072)
	at org.apache.hudi.HoodieSparkSqlWriterInternal.writeInternal(HoodieSparkSqlWriter.scala:520)
	at org.apache.hudi.HoodieSparkSqlWriterInternal.write(HoodieSparkSqlWriter.scala:204)
	at org.apache.hudi.HoodieSparkSqlWriter$.write(HoodieSparkSqlWriter.scala:121)
	at org.apache.hudi.DefaultSource.createRelation(DefaultSource.scala:150)
	at org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand.run(SaveIntoDataSourceCommand.scala:47)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult$lzycompute(commands.scala:75)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult(commands.scala:73)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.executeCollect(commands.scala:84)
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:98)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:118)
	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:195)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:103)
	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:827)
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:65)
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:98)
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:94)
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:512)
	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:104)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:512)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:31)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:31)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:31)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:488)
	at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:94)
	at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:81)
	at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:79)
	at org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:133)
	at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:856)
	at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:387)
	at org.apache.spark.sql.DataFrameWriter.saveInternal(DataFrameWriter.scala:360)
	at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:239)
	at com.example.hudi.CleanReproduction$.delayedEndpoint$com$example$hudi$CleanReproduction$1(CleanReproduction.scala:89)
	at com.example.hudi.CleanReproduction$delayedInit$body.apply(CleanReproduction.scala:7)
	at scala.Function0.apply$mcV$sp(Function0.scala:39)
	at scala.Function0.apply$mcV$sp$(Function0.scala:39)
	at scala.runtime.AbstractFunction0.apply$mcV$sp(AbstractFunction0.scala:17)
	at scala.App.$anonfun$main$1$adapted(App.scala:80)
	at scala.collection.immutable.List.foreach(List.scala:431)
	at scala.App.main(App.scala:80)
	at scala.App.main$(App.scala:78)
	at com.example.hudi.CleanReproduction$.main(CleanReproduction.scala:7)
	at com.example.hudi.CleanReproduction.main(CleanReproduction.scala)
Caused by: org.apache.hudi.exception.HoodieUpsertException: Error upserting bucketType UPDATE for partition :0
	at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpsertPartition(BaseSparkCommitActionExecutor.java:342)
	at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.lambda$mapPartitionsAsRDD$a3ab3c4$1(BaseSparkCommitActionExecutor.java:257)
	at org.apache.spark.api.java.JavaRDDLike.$anonfun$mapPartitionsWithIndex$1(JavaRDDLike.scala:102)
	at org.apache.spark.api.java.JavaRDDLike.$anonfun$mapPartitionsWithIndex$1$adapted(JavaRDDLike.scala:102)
	at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsWithIndex$2(RDD.scala:905)
	at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsWithIndex$2$adapted(RDD.scala:905)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:364)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:328)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:364)
	at org.apache.spark.rdd.RDD.$anonfun$getOrCompute$1(RDD.scala:377)
	at org.apache.spark.storage.BlockManager.$anonfun$doPutIterator$1(BlockManager.scala:1552)
	at org.apache.spark.storage.BlockManager.org$apache$spark$storage$BlockManager$$doPut(BlockManager.scala:1462)
	at org.apache.spark.storage.BlockManager.doPutIterator(BlockManager.scala:1526)
	at org.apache.spark.storage.BlockManager.getOrElseUpdate(BlockManager.scala:1349)
	at org.apache.spark.rdd.RDD.getOrCompute(RDD.scala:375)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:326)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:364)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:328)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:92)
	at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:161)
	at org.apache.spark.scheduler.Task.run(Task.scala:139)
	at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:554)
	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1529)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:557)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
Caused by: org.apache.hudi.exception.HoodieException: org.apache.hudi.exception.HoodieException: org.apache.hudi.exception.HoodieUpsertException: Failed to merge old record into new file for key 0000000000004372619 from old file /tmp/reproduction/__HIVE_DEFAULT_PARTITION__/__HIVE_DEFAULT_PARTITION__/__HIVE_DEFAULT_PARTITION__/f686ea7b-09d2-46cc-804c-7ba006e386c6-0_0-20-111_20240613122842797.parquet to new file /tmp/reproduction/__HIVE_DEFAULT_PARTITION__/__HIVE_DEFAULT_PARTITION__/__HIVE_DEFAULT_PARTITION__/f686ea7b-09d2-46cc-804c-7ba006e386c6-0_0-20-124_20240613122855601.parquet with writerSchema {
  "type" : "record",
  "name" : "assessment_questions_record",
  "namespace" : "hoodie.assessment_questions",
  "fields" : [ {
    "name" : "_hoodie_commit_time",
    "type" : [ "null", "string" ],
    "doc" : "",
    "default" : null
  }, {
    "name" : "_hoodie_commit_seqno",
    "type" : [ "null", "string" ],
    "doc" : "",
    "default" : null
  }, {
    "name" : "_hoodie_record_key",
    "type" : [ "null", "string" ],
    "doc" : "",
    "default" : null
  }, {
    "name" : "_hoodie_partition_path",
    "type" : [ "null", "string" ],
    "doc" : "",
    "default" : null
  }, {
    "name" : "_hoodie_file_name",
    "type" : [ "null", "string" ],
    "doc" : "",
    "default" : null
  }, {
    "name" : "meta",
    "type" : {
      "type" : "record",
      "name" : "meta",
      "namespace" : "hoodie.assessment_questions.assessment_questions_record",
      "fields" : [ {
        "name" : "lsn",
        "type" : [ "null", "long" ],
        "default" : null
      } ]
    }
  }, {
    "name" : "internal_list",
    "type" : {
      "type" : "array",
      "items" : "long"
    }
  }, {
    "name" : "hkey",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "partition",
    "type" : [ "null", "string" ],
    "default" : null
  } ]
}
	at org.apache.hudi.table.action.commit.HoodieMergeHelper.runMerge(HoodieMergeHelper.java:149)
	at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpdateInternal(BaseSparkCommitActionExecutor.java:387)
	at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpdate(BaseSparkCommitActionExecutor.java:369)
	at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpsertPartition(BaseSparkCommitActionExecutor.java:335)
	... 29 more
Caused by: org.apache.hudi.exception.HoodieException: org.apache.hudi.exception.HoodieUpsertException: Failed to merge old record into new file for key 0000000000004372619 from old file /tmp/reproduction/__HIVE_DEFAULT_PARTITION__/__HIVE_DEFAULT_PARTITION__/__HIVE_DEFAULT_PARTITION__/f686ea7b-09d2-46cc-804c-7ba006e386c6-0_0-20-111_20240613122842797.parquet to new file /tmp/reproduction/__HIVE_DEFAULT_PARTITION__/__HIVE_DEFAULT_PARTITION__/__HIVE_DEFAULT_PARTITION__/f686ea7b-09d2-46cc-804c-7ba006e386c6-0_0-20-124_20240613122855601.parquet with writerSchema {
  "type" : "record",
  "name" : "assessment_questions_record",
  "namespace" : "hoodie.assessment_questions",
  "fields" : [ {
    "name" : "_hoodie_commit_time",
    "type" : [ "null", "string" ],
    "doc" : "",
    "default" : null
  }, {
    "name" : "_hoodie_commit_seqno",
    "type" : [ "null", "string" ],
    "doc" : "",
    "default" : null
  }, {
    "name" : "_hoodie_record_key",
    "type" : [ "null", "string" ],
    "doc" : "",
    "default" : null
  }, {
    "name" : "_hoodie_partition_path",
    "type" : [ "null", "string" ],
    "doc" : "",
    "default" : null
  }, {
    "name" : "_hoodie_file_name",
    "type" : [ "null", "string" ],
    "doc" : "",
    "default" : null
  }, {
    "name" : "meta",
    "type" : {
      "type" : "record",
      "name" : "meta",
      "namespace" : "hoodie.assessment_questions.assessment_questions_record",
      "fields" : [ {
        "name" : "lsn",
        "type" : [ "null", "long" ],
        "default" : null
      } ]
    }
  }, {
    "name" : "internal_list",
    "type" : {
      "type" : "array",
      "items" : "long"
    }
  }, {
    "name" : "hkey",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "partition",
    "type" : [ "null", "string" ],
    "default" : null
  } ]
}
	at org.apache.hudi.common.util.queue.SimpleExecutor.execute(SimpleExecutor.java:75)
	at org.apache.hudi.table.action.commit.HoodieMergeHelper.runMerge(HoodieMergeHelper.java:147)
	... 32 more
Caused by: org.apache.hudi.exception.HoodieUpsertException: Failed to merge old record into new file for key 0000000000004372619 from old file /tmp/reproduction/__HIVE_DEFAULT_PARTITION__/__HIVE_DEFAULT_PARTITION__/__HIVE_DEFAULT_PARTITION__/f686ea7b-09d2-46cc-804c-7ba006e386c6-0_0-20-111_20240613122842797.parquet to new file /tmp/reproduction/__HIVE_DEFAULT_PARTITION__/__HIVE_DEFAULT_PARTITION__/__HIVE_DEFAULT_PARTITION__/f686ea7b-09d2-46cc-804c-7ba006e386c6-0_0-20-124_20240613122855601.parquet with writerSchema {
  "type" : "record",
  "name" : "assessment_questions_record",
  "namespace" : "hoodie.assessment_questions",
  "fields" : [ {
    "name" : "_hoodie_commit_time",
    "type" : [ "null", "string" ],
    "doc" : "",
    "default" : null
  }, {
    "name" : "_hoodie_commit_seqno",
    "type" : [ "null", "string" ],
    "doc" : "",
    "default" : null
  }, {
    "name" : "_hoodie_record_key",
    "type" : [ "null", "string" ],
    "doc" : "",
    "default" : null
  }, {
    "name" : "_hoodie_partition_path",
    "type" : [ "null", "string" ],
    "doc" : "",
    "default" : null
  }, {
    "name" : "_hoodie_file_name",
    "type" : [ "null", "string" ],
    "doc" : "",
    "default" : null
  }, {
    "name" : "meta",
    "type" : {
      "type" : "record",
      "name" : "meta",
      "namespace" : "hoodie.assessment_questions.assessment_questions_record",
      "fields" : [ {
        "name" : "lsn",
        "type" : [ "null", "long" ],
        "default" : null
      } ]
    }
  }, {
    "name" : "internal_list",
    "type" : {
      "type" : "array",
      "items" : "long"
    }
  }, {
    "name" : "hkey",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "partition",
    "type" : [ "null", "string" ],
    "default" : null
  } ]
}
	at org.apache.hudi.io.HoodieMergeHandle.write(HoodieMergeHandle.java:379)
	at org.apache.hudi.table.action.commit.BaseMergeHelper$UpdateHandler.consume(BaseMergeHelper.java:54)
	at org.apache.hudi.table.action.commit.BaseMergeHelper$UpdateHandler.consume(BaseMergeHelper.java:44)
	at org.apache.hudi.common.util.queue.SimpleExecutor.execute(SimpleExecutor.java:69)
	... 33 more
Caused by: java.lang.RuntimeException: Null-value for required field: internal_list
	at org.apache.parquet.avro.AvroWriteSupport.writeRecordFields(AvroWriteSupport.java:203)
	at org.apache.parquet.avro.AvroWriteSupport.write(AvroWriteSupport.java:174)
	at org.apache.parquet.hadoop.InternalParquetRecordWriter.write(InternalParquetRecordWriter.java:138)
	at org.apache.parquet.hadoop.ParquetWriter.write(ParquetWriter.java:310)
	at org.apache.hudi.io.storage.HoodieBaseParquetWriter.write(HoodieBaseParquetWriter.java:147)
	at org.apache.hudi.io.storage.HoodieAvroParquetWriter.writeAvro(HoodieAvroParquetWriter.java:76)
	at org.apache.hudi.io.storage.HoodieAvroFileWriter.write(HoodieAvroFileWriter.java:51)
	at org.apache.hudi.io.storage.HoodieFileWriter.write(HoodieFileWriter.java:43)
	at org.apache.hudi.io.HoodieMergeHandle.writeToFile(HoodieMergeHandle.java:393)
	at org.apache.hudi.io.HoodieMergeHandle.write(HoodieMergeHandle.java:374)
	... 36 more
```