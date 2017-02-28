package com.quantifind.utils

import scala.util.{Failure, Success, Try}

/**
 * Basic utils, e.g. retry block
 *
 * @author xorlev
 */

object Utils {

  // Returning T, throwing the exception on failure
  @annotation.tailrec
  final def retry[T](n: Int)(fn: => T): T = {
    Try { fn } match {
      case Success(x) => x
      case _ if n > 1 => retry(n - 1)(fn)
      case Failure(e) => throw e
    }
  }
}
