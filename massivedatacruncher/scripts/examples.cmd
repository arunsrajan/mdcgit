call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceIgnite  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceJGroups  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceJGroupsDivided  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceNormalInMemory hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceNormalInMemoryDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceNormalInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceNormalInMemoryImplicit hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc 1 11 18432

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceNormalJGroupsImplicit hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc 1 11 18432

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceNormalInMemoryDiskDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceNormalDisk hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceNormalDiskDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceYARN hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceNormalInMemorySortByDelayResourceDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceNormalInMemorySortByAirlinesResourceDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.json.GithubEventsStreamReduce hdfs://127.0.0.1:9000 /github /examplesmdc sa

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.json.GithubEventsStreamReduce hdfs://127.0.0.1:9000 /github /examplesmdc local

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.json.GithubEventsStreamReduce hdfs://127.0.0.1:9000 /github /examplesmdc yarn

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.json.GithubEventsStreamReduce hdfs://127.0.0.1:9000 /github /examplesmdc jgroups

REM Stream Reduce LOJ

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceLeftOuterJoinIgnite  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceLeftOuterJoinJGroups  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceLeftOuterJoinJGroupsDivided  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceLeftOuterJoinNormal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceLeftOuterJoinNormalDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceLeftOuterJoinYARN hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceLeftOuterJoinInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc 18432 3 128

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceLeftOuterJoinInMemoryDiskDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc 18432 3 128


REM Stream Reduce ROJ


call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceRightOuterJoinIgnite  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceRightOuterJoinJGroups  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceRightOuterJoinJGroupsDivided  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceRightOuterJoinNormal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceRightOuterJoinNormalDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceRightOuterJoinYARN hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceRightOuterJoinInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc 18432 3 128

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamReduceRightOuterJoinInMemoryDiskDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc 18432 3 128

REM Stream Reduce Transformations

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReduceSample hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReduceCoalesceOne hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReduceCoalescePartition hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReduceUnion hdfs://127.0.0.1:9000 /airline1989 /1987 /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReduceIntersection hdfs://127.0.0.1:9000 /airline1989 /1987 /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReduceCachedIgnite hdfs://127.0.0.1:9000 /airline1989 /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReducePairJoin hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReducePairLeftJoin hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReducePairRightJoin hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReducePairJoinCoalesceReduction hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReducePairLeftJoinCoalesceReduction hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReducePairRightJoinCoalesceReduction hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReducePairMultipleJoinsCoalesceReduction hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReduceSampleLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReduceCoalesceOneLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReduceCoalescePartitionLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReduceUnionLocal hdfs://127.0.0.1:9000 /airline1989 /1987 /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReduceIntersectionLocal hdfs://127.0.0.1:9000 /airline1989 /1987 /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReduceCachedIgniteLocal hdfs://127.0.0.1:9000 /airline1989 /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReducePairJoinLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReducePairLeftJoinLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReducePairRightJoinLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReducePairJoinCoalesceReductionLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReducePairLeftJoinCoalesceReductionLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReducePairRightJoinCoalesceReductionLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.transformation.examples.StreamReducePairMultipleJoinsCoalesceReductionLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesmdc

REM Stream Reduce SQL

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlSumLocal hdfs://127.0.0.1:9000 /airline1989 /carriers 128 11

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlSumSAInMemory hdfs://127.0.0.1:9000 /airline1989 /carriers 128 11

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlSumSAInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /carriers 128 11

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlSumSADisk hdfs://127.0.0.1:9000 /airline1989 /carriers 128 11

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlCountLocal hdfs://127.0.0.1:9000 /airline1989 128 11

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierSumCountArrDelayLocal hdfs://127.0.0.1:9000 /airline1989 128 11

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySADisk hdfs://127.0.0.1:9000 /airline1989 128 11

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySAInMemory hdfs://127.0.0.1:9000 /airline1989 128 11


call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySAInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 128 11

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierSumCountDepDelayLocal hdfs://127.0.0.1:9000 /airline1989 128 11

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySADisk hdfs://127.0.0.1:9000 /airline1989 128 11 18432 1

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySAInMemory hdfs://127.0.0.1:9000 /airline1989 128 11 18432 1

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySAInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 128 11 18432 1

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierYearMonthOfYearDayOfMonthSumCountArrDelayLocal hdfs://127.0.0.1:9000 /airline1989 128 11

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierYearMonthOfYearDayOfMonthSumCountArrDelaySaveLocal hdfs://127.0.0.1:9000 /airline1989 128 11 /examplesmdc

Stream Reduce Aggregate
-----------------------

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamAggSumCountArrDelayDisk hdfs://127.0.0.1:9000 /airline1989 /examplesmdc 18432 1

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamAggSumCountArrDelayDiskDivided hdfs://127.0.0.1:9000 /airline1989 /examplesmdc 18432 3

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamAggSumCountArrDelayIgnite hdfs://127.0.0.1:9000 /airline1989 /examplesmdc 18432 1

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamAggSumCountArrDelayInMemory hdfs://127.0.0.1:9000 /airline1989 /examplesmdc 18432 1

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamAggSumCountArrDelayInMemoryDivided hdfs://127.0.0.1:9000 /airline1989 /examplesmdc 18432 3

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamAggSumCountArrDelayInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /examplesmdc 18432 1

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamAggSumCountArrDelayInMemoryDiskDivided hdfs://127.0.0.1:9000 /airline1989 /examplesmdc 18432 3

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamAggSumCountArrDelayCoalesceInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /examplesmdc 18432 1

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamAggSumCountArrDelayCoalesceInMemoryDiskDivided hdfs://127.0.0.1:9000 /airline1989 /examplesmdc 18432 3

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamAggSumCountArrDelayJGroups hdfs://127.0.0.1:9000 /airline1989 /examplesmdc 18432 1

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamAggSumCountArrDelayJGroupsDivided hdfs://127.0.0.1:9000 /airline1989 /examplesmdc 18432 3

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamAggSumCountArrDelayLocal hdfs://127.0.0.1:9000 /airline1989 /examplesmdc 18432 1

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamAggSumCountArrDelayYARN hdfs://127.0.0.1:9000 /airline1989 /examplesmdc 18432 1


call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamCoalesceNormalInMemoryDiskContainerDivided hdfs://127.0.0.1:9000 /airline1989 /examplesmdc 1

REM Filter Operation Streaming

call streamjobsubmitter.cmd ../modules/examples.jar com.github.mdc.stream.examples.StreamFilterFilterCollectArrDelayInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /examplesmdc 18432 1 32


REM Running MR Job examples

call mapreducejobsubmitter.cmd -jar ../modules/examples.jar -args "com.github.mdc.mr.examples.join.MrJobArrivalDelayNormal /airline1989 /carriers /examplesmdc 128 10"

call mapreducejobsubmitter.cmd -jar ../modules/examples.jar -args "com.github.mdc.mr.examples.join.MrJobArrivalDelayYARN /airline1989 /carriers /examplesmdc 3 18432" 

call mapreducejobsubmitter.cmd -jar ../modules/examples.jar -args "com.github.mdc.mr.examples.join.MrJobArrivalDelayIGNITE /airline1989 /carriers /examplesmdc"


pause