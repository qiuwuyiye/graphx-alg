package NMF

/**
 * Created by liping on 8/14/15.
 */
case class ParaConfig(
  edgesFile: String = "hdfs://bda00:8020/user/liping/edges.txt",
  output: String = "hdfs://bda00:8020/user/liping/NMFResult",
  edgesMinPartition: Int = 2,
  reducedDim: Int = 20,
  learnRate: Double = 0.01,
  reg: Double = 0.1,
  maxIteration: Int = 40
)
