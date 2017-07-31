import java.lang._
import java.util.Properties
import scala.io.Source
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.mutable.HashSet
import scala.collection.mutable.Set
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.types._
import java.util.Date 
import java.text.SimpleDateFormat
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix

object entropy {

	val targetIP:String = "202.121.223.4"
	val maxNumPerSecond:Int = 10000

	def main(args: Array[String]){
		//主函数参数，指定输入数据文件路径
		//check parameters
        if(args.length < 1){
            println("Usage: entropy dataFilePath")
            sys.exit(1)
        }

        val conf = new SparkConf().setAppName("EntropyAnomaly")
    	val sc = new SparkContext(conf)
    	val sqlContext = new SQLContext(sc)

        val filePath = args(0)
		var index:Int = 0
		var first = true
		var lastTime = ""
		var oneWay:Int = 0
		var revWay:Int = 0
		var packetSizeSum = 0
		var packetNumSum = 0

		var packetSizeArray = ArrayBuffer[Int]()
		var packetNumArray = ArrayBuffer[Int]()
		//Using timestamp as primary key when write into database
		val packetTimeArray = ArrayBuffer[String]()

		var entropyDstPort:Array[Double] = new Array[Double](maxNumPerSecond)
		var entropySrcPort:Array[Double] = new Array[Double](maxNumPerSecond)
		var entropyDstIp:Array[Double] = new Array[Double](maxNumPerSecond)
		var entropySize:Array[Double] = new Array[Double](maxNumPerSecond)

		var entropyDstPortPassive:Array[Double] = new Array[Double](maxNumPerSecond)
		var entropySrcPortPassive:Array[Double] = new Array[Double](maxNumPerSecond)
		var entropySrcIpPassive:Array[Double] = new Array[Double](maxNumPerSecond)
		var entropySizePassive:Array[Double] = new Array[Double](maxNumPerSecond)

		var dstPort:Map[String, Int] = new HashMap[String, Int]
		var dstIp:Map[String, Int] = new HashMap[String, Int]
		var srcPort:Map[String, Int] = new HashMap[String, Int]
		var size:Map[String, Int] = new HashMap[String, Int]
		var dstPortPassive:Map[String, Int] = new HashMap[String, Int]
		var srcIpPassive:Map[String, Int] = new HashMap[String, Int]
		var srcPortPassive:Map[String, Int] = new HashMap[String, Int]
		var sizePassive:Map[String, Int] = new HashMap[String, Int]


		//get entropy matrix
		//val rawData = sc.textFile(filePath)
		val lines = Source.fromFile(filePath).getLines
		for(line <- lines){
			var strArray = line.split('\t')
			if(first){
				first = false
				lastTime = strArray(0)
                //println("[debug] This is first line: ")
                strArray.foreach{t => println(t)}
			}
			if(strArray(0) != lastTime){
				packetSizeArray += packetSizeSum
				packetNumArray += packetNumSum
				packetTimeArray += lastTime

				entropyDstPort(index) = getEntropy(dstPort, oneWay)
				entropyDstIp(index) = getEntropy(dstIp, oneWay)
				entropySrcPort(index) = getEntropy(srcPort, oneWay)
				entropySize(index) = getEntropy(size, oneWay)

		        entropyDstPortPassive(index) = getEntropy(dstPortPassive, revWay)
		        entropySrcIpPassive(index) = getEntropy(srcIpPassive, revWay)
		        entropySrcPortPassive(index) = getEntropy(srcPortPassive, revWay)
		        entropySizePassive(index) = getEntropy(sizePassive, revWay)
		        
				dstPort.clear()
				dstIp.clear()
				srcPort.clear()
				srcIpPassive.clear()
				srcPortPassive.clear()
				dstPortPassive.clear()
				size.clear()
				sizePassive.clear()
		        //forHighCharts(index) = packetSizeSum
		        packetSizeSum = 0;packetNumSum = 0;oneWay = 0;revWay = 0
		        lastTime = strArray(0)
		        index += 1
			}
			if(strArray(1) == targetIP){
				processEntropy(dstPort, strArray(4))
		        processEntropy(dstIp, strArray(2))
		        processEntropy(srcPort, strArray(3))
		        processEntropy(size,QuatitizeSize(strArray(6)))
		        oneWay += 1
			}
			else{
				processEntropy(srcIpPassive,strArray(1))
		        processEntropy(srcPortPassive,strArray(3))
		        processEntropy(dstPortPassive,strArray(4))
		        processEntropy(sizePassive,QuatitizeSize(strArray(6)))
		        revWay += 1
			}
			packetSizeSum += strArray(6).toInt
    		packetNumSum += 1
		}

        //PCA
        //1.entropy array to matrix
        //var data:Array[Vector[Double]] = new Array[Vector[Double]](maxNumPerSecond)
        var data = ArrayBuffer[Vector]()
        println("[debug] The value of index is " + index)
        for(i <- 0 to index-1)
        {
            //println("[debug] This time value of i is "+ i)
        	data += Vectors.dense(entropySize(i),entropySizePassive(i),entropySrcIpPassive(i),entropySrcPortPassive(i),entropyDstPortPassive(i))
        }
        val rows = sc.parallelize(data)
        val mat: RowMatrix = new RowMatrix(rows)
        val pc: Matrix = mat.computePrincipalComponents(2)
        val projected: RowMatrix = mat.multiply(pc)
        val collect = projected.rows.collect()  //collect Array[Vector]
        //println("Projected Row Matrix of principal component:")
        //collect.foreach{ vector => println(vector) }

        //preparation for writing into database
        val colArray = collect.map(vector => {
        	vector.toArray
        })

        //Get anomaly points and write into database. Including 3 parts(time-traffic, time-entropy, time-PCA-2-dimensions and initial data)
        val prop = new Properties()
        prop.put("user", "root")
        prop.put("password", "hadoop928")
        prop.put("driver", "com.mysql.jdbc.Driver")
        val bytesSchema = StructType(List( 
                                    StructField("bytes_time", StringType, true), 
                                    StructField("packet_bytes", StringType, true),
                                    StructField("packet_number", StringType, true)
                                ))
        val entropySchema = StructType(List(
                                    StructField("entropy_time", StringType, true),
                                    StructField("entropy_srcip", StringType, true),
                                    StructField("entropy_srcport", StringType, true),
                                    StructField("entropy_dstport", StringType, true)
                                ))
        val pcaSchema = StructType(List(
                                    StructField("pca_time", StringType, true),
                                    StructField("pca_d1", StringType, true),
                                    StructField("pca_d2", StringType, true)
                                ))

        var pcaArray = new ArrayBuffer[Array[String]]()
        for(i <- 0 to colArray.length-1){
            pcaArray += Array(packetTimeArray(i), colArray(i).apply(0).toString, colArray(i).apply(1).toString)
        }
        val pcaRDD = sc.parallelize(pcaArray)
        val pcaRowRDD = pcaRDD.map({ case(array) =>
            Row(array(0).trim, array(1).trim, array(2).trim)
        })
        val pcaDataFrame = sqlContext.createDataFrame(pcaRowRDD, pcaSchema)
        pcaDataFrame.write.mode("append").jdbc("jdbc:mysql://10.255.0.12:3306/entropy", "entropy.pcaTime", prop)

        //packetTimeArray :: packetSizeArray parallelize => RDD 
        var bytesArray = new ArrayBuffer[Array[String]]()
        for(i <- 0 to packetTimeArray.length-1){
            bytesArray += Array(packetTimeArray(i), packetSizeArray(i).toString, packetNumArray(i).toString)
        }
        val bytesRDD = sc.parallelize(bytesArray)
        val bytesRowRDD = bytesRDD.map({ case(array) => 
            Row(array(0).toString.trim, array(1).toString.trim, array(2).toString.trim)
        })
        val bytesDataFrame = sqlContext.createDataFrame(bytesRowRDD, bytesSchema)
        bytesDataFrame.write.mode("append").jdbc("jdbc:mysql://10.255.0.12:3306/entropy", "entropy.bytesNumTime", prop)

        var entropyArray = new ArrayBuffer[Array[String]]()
        for(i <- 0 to packetTimeArray.length-1){
            entropyArray += Array(packetTimeArray(i), entropySrcIpPassive(i).toString, entropySrcPortPassive(i).toString, entropyDstPortPassive(i).toString)
            //println("[debug] EntropySrcIpPassive["+ i + "] = " + entropySrcIpPassive(i).toString)
        }
        val entropyRDD = sc.parallelize(entropyArray)
        val entropyRowRDD = entropyRDD.map({ case(array) => 
            Row(array(0).trim, array(1).trim, array(2).trim, array(3).trim)
        })
        val entropyDataFrame = sqlContext.createDataFrame(entropyRowRDD, entropySchema)
        entropyDataFrame.write.mode("append").jdbc("jdbc:mysql://10.255.0.12:3306/entropy", "entropy.entropyTime", prop)

        sc.stop()
	}

    //mutable map problem need to be solved...
    def processEntropy(theDict: Map[String, Int], value: String){
    	if(theDict.contains(value)){
    		theDict(value) = theDict.apply(value) + 1
    	}
    	else{
    		theDict(value) = 1
    	}
    }

    //数据类型需要确定
    def getEntropy(theDict: Map[String, Int], packNum: Int):Double = {
    	var result = 0.0
    	for(v <- theDict.values){
    		result += (-1*(1.0*v/packNum)*Math.log(1.0*v/packNum))
        }
    	return result
    }

    def QuatitizeSize(sizee: String):String = {
    	val size = sizee.toInt
        val result = (size/100+1)*100
        return result.toString 
    }

}
