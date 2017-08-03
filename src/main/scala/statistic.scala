package scala

import java.lang._
import scala.io.Source
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.mutable.HashSet
import scala.collection.mutable.Set
import scala.collection.mutable.ArrayBuffer

class statis(){
	val points:ArrayBuffer[Array[String]] = 

	def computeDistance():Double = {

	}

	def getSortedDistanceArray():Array[Array[String]] = {

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