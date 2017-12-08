package org.apache.spark

import scala.collection.mutable.Stack

class SetSuite extends SparkFunSuite {

  test("Fuck me") {
    val stack = new Stack[Int]
    stack.push(1)
    stack.push(2)
    assert(stack.pop() === 2)
    assert(stack.pop() === 1)
  }

  test("Fuck you") {
    val emptyStack = new Stack[String]
    assertThrows[NoSuchElementException] {
      emptyStack.pop()
    }
  }
}

