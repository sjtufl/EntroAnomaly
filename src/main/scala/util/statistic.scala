package util

import java.lang._
import scala.io.Source
import scala.math._
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.mutable.HashSet
import scala.collection.mutable.Set
import scala.collection.mutable.ArrayBuffer

class statis(data:ArrayBuffer[Array[String]]){

    val pcadata:ArrayBuffer[Array[String]] = data

    //计算到原点的距离
    def computeDistance(x:Double, y:Double):Double = {
        return math.sqrt((x*x) + (y*y))
    }

    def getSortedDistanceArray():Array[Array[String]] = {
        
        var pcaDistData = pcadata.map({
            case(point) =>
                val x = point(1).toDouble
                val y = point(2).toDouble
                val dist = computeDistance(x, y)
                (dist, point(1), point(2))
            }).sortBy({case(dist, x, y) => dist})

        val res = pcaDistData.map({
            case(dist, x, y) =>
            Array(x, y)
        })

        return res.toArray
    }

    def _topx(x:Int):Array[Array[String]] = {
        val sortedArray = getSortedDistanceArray()
        val res = new ArrayBuffer[Array[String]]()
        for(i <- 0 to x-1){
            res += sortedArray.apply(i)
        }
        return res.toArray
    }
}