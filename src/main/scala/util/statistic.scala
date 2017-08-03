package util

import java.lang._
import scala.io.Source
import scala.math._
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.mutable.HashSet
import scala.collection.mutable.Set
import scala.collection.mutable.ArrayBuffer

class statis(data:Array[Array[String]]){

    val pcadata:Array[Array[String]] = data

    //计算到原点的距离
    def computeDistance(x:Double, y:Double):Double = {
        return math.sqrt((x*x) + (y*y))
    }

    def getSortedDistanceArray():Array[Array[String]] = {
        
        var pcaDistData = pcadata.map({
            case(point) =>
                val x = point(0).toDouble
                val y = point(1).toDouble
                val time = point(2)
                val dist = computeDistance(x, y)
                (dist, point(0), point(1), time)
            }).sortBy({case(dist, x, y, time) => dist})

        val res = pcaDistData.map({
            case(dist, x, y, time) =>
            Array(x, y, time)
        })

        return res
    }

    def _topx(x:Int):Array[Array[String]] = {
        val sortedArray = getSortedDistanceArray().reverse
        val res = new ArrayBuffer[Array[String]]()
        for(i <- 0 to x-1){
            res += sortedArray.apply(i)
        }
        return res.toArray
    }
}