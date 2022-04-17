package sql42

class UDFReigstry {

}

trait Function1UDF[A, B] {
  def f(a: A): B
}

class ArrayContains {
  def f[T](a: Array[T], value: T): Boolean = ???
}

class isNull {
  def f[T](a: T): T = a
}