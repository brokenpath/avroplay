package com.bench.avroplay

import org.scalacheck.Gen
import com.bench.avrotypes._

trait HdfsFixture {
  val genFileName: Gen[String] = Gen.identifier
  val genChunk: Gen[Array[Byte]] = Gen.identifier.map(_.getBytes)
  val genChunks: Gen[List[Array[Byte]]] = Gen.listOfN(10, genChunk)


  val genWithSchemaProp = for {
    str1 <- Gen.alphaStr
    str2 <- Gen.option(Gen.alphaUpperStr)
    str3 <- Gen.option(Gen.alphaNumStr)
  } yield WithSchemaProp(str1, str2, str3)


  val genWithSchemaPropFiles = Gen.nonEmptyListOf(Gen.listOf(genWithSchemaProp))  


  val gen4Tuple = for {
    v1 <-  Gen.alphaStr
    v2 <- Gen.option(Gen.alphaLowerStr)
    v3 <- Gen.option(Gen.alphaNumStr)
    v4 <- Gen.option(Gen.alphaUpperStr)
  } yield (v1, v2, v3, v4)

  def genListOf4Tuple(n : Int) = Gen.listOfN(n, gen4Tuple)
  
  def getListOfStr(n: Int) = Gen.listOfN(n, Gen.alphaStr)

}
