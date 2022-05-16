package sql42

object UDFReigstry {

}

class ArrayContains {
  def f[T](a: Array[T], value: T): Boolean = ???
}

class isNull {
  def f[T](a: T): T = a
}

class MySum() {
  def init = 0
  def add(accumulator: Int, v: Int): Int = accumulator + v
  def merge(accumulator0: Int, accumulator1: Int): Int = accumulator0 + accumulator1
  def result(accumulator: Int): Int = accumulator
}
