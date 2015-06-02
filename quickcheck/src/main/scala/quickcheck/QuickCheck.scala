package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }
  
  property("minof2") = forAll { (a: Int, b: Int) =>
    val h = insert(a, empty)
    val i = insert(b, h)
    findMin(i) == List(a, b).min
  }
  
  property("delafterins") = forAll { a: Int =>
    val h = insert(a, empty)
    val i = deleteMin(h)
    i == empty
  }
  
  property("sortedseq") = forAll { list: List[Int] =>
    val h = genHeapFromList(empty, list)
    val sortedByDeleteMin = deleteMinRecursive(h, List.empty)
    sortedByDeleteMin == list.sorted
  }
  
  property("minofeither") = forAll { (h1: H, h2: H) =>
    val h = meld(h1, h2)
    val meldedMin = findMin(h) 
    meldedMin == findMin(h1) || meldedMin == findMin(h2) 
  }

  lazy val genHeap: Gen[H] = for {
    a <- arbitrary[A]
    h <- insert(a, empty)
  } yield h

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)
  
  def genHeapFromList(h: H, s: List[Int]): H = {
    if (s.isEmpty)
      h
    else {
      genHeapFromList(insert(s.head, h), s.tail)
    }
  }

  def deleteMinRecursive(h: H, s: List[Int]): List[Int] = {
    if (isEmpty(h))
      s
    else {
      val newS = s :+ findMin(h)
      val newH = deleteMin(h)
      deleteMinRecursive(newH, newS)
    }
  }
}
