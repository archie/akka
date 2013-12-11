package akka.testkit

import org.scalatest.Matchers
import org.scalatest.{ BeforeAndAfterEach, WordSpec }
import scala.concurrent.duration._
import com.typesafe.config.Config
import org.scalatest.exceptions.TestFailedException

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class TestTimeSpec extends AkkaSpec(Map("akka.test.timefactor" -> 2.0)) with BeforeAndAfterEach {

  "A TestKit" should {

    "correctly dilate times" taggedAs TimingTest in {
      val probe = TestProbe()
      val now = System.nanoTime
      intercept[AssertionError] { probe.awaitCond(false, Duration("1 second")) }
      val diff = System.nanoTime - now
      val target = (1000000000l * testKitSettings.TestTimeFactor).toLong
      diff should be > (target - 300000000l)
      diff should be < (target + 300000000l)
    }

    "awaitAssert should throw correctly" in {
      awaitAssert("foo" should be("foo"))
      within(300.millis, 2.seconds) {
        intercept[TestFailedException] {
          awaitAssert("foo" should be("bar"), 500.millis, 300.millis)
        }
      }
    }

  }

}
