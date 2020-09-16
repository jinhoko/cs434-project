import org.scalatest.FunSuite

class CubeCalculatorTest extends FunSuite {
  test("CubeCalculator.cube") {
    assert(CubeCalculator.cube(3) === 27)
  }
}