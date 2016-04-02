package com.github.agourlay.cornichon.core

import cats.data.Xor
import cats.data.Xor.{ left, right }
import com.github.agourlay.cornichon.dsl._
import com.github.agourlay.cornichon.core.ScenarioReport._

import scala.annotation.tailrec
import scala.concurrent.{ ExecutionContext, Await, Future }
import scala.concurrent.duration.Duration
import scala.util._

class Engine(executionContext: ExecutionContext) {

  private implicit val ec = executionContext

  def runScenario(session: Session, finallySteps: Seq[Step] = Seq.empty)(scenario: Scenario): ScenarioReport = {
    val initMargin = 1
    val titleLog = Vector(DefaultLogInstruction(s"Scenario : ${scenario.name}", initMargin))
    val mainExecution = fromStepsReport(scenario, runSteps(scenario.steps, session, titleLog, initMargin + 1))
    if (finallySteps.isEmpty)
      mainExecution
    else {
      val finallyExecution = fromStepsReport(scenario, runSteps(finallySteps.toVector, mainExecution.session, Vector.empty, initMargin + 1))
      mainExecution.merge(finallyExecution)
    }
  }

  private[cornichon] def runSteps(steps: Vector[Step], session: Session, logs: Vector[LogInstruction], depth: Int): StepsReport =
    steps.headOption.fold[StepsReport](SuccessRunSteps(session, logs)) {
      case d @ DebugStep(message) ⇒
        Try { message(session) } match {
          case Success(debugMessage) ⇒
            val updatedLogs = logs :+ InfoLogInstruction(message(session), depth)
            runSteps(steps.tail, session, updatedLogs, depth)
          case Failure(e) ⇒
            val cornichonError = toCornichonError(e)
            val updatedLogs = logs ++ errorLogs(d.title, cornichonError, depth, steps.tail)
            buildFailedRunSteps(steps, d, cornichonError, updatedLogs, session)
        }

      case e @ EventuallyStart(conf) ⇒
        val updatedLogs = logs :+ DefaultLogInstruction(e.title, depth)
        val eventuallySteps = findEnclosedSteps(e, steps.tail)

        if (eventuallySteps.isEmpty) {
          val updatedLogs = logs ++ errorLogs(e.title, MalformedEventuallyBlock, depth, steps.tail)
          buildFailedRunSteps(steps, e, MalformedEventuallyBlock, updatedLogs, session)
        } else {
          val (res, executionTime) = withDuration {
            retryEventuallySteps(eventuallySteps, session, conf, Vector.empty, depth + 1)
          }
          val nextSteps = steps.tail.drop(eventuallySteps.size)
          res match {
            case s @ SuccessRunSteps(sSession, sLogs) ⇒
              val fullLogs = updatedLogs ++ sLogs :+ SuccessLogInstruction(s"Eventually block succeeded", depth, Some(executionTime))
              runSteps(nextSteps, sSession, fullLogs, depth)
            case f @ FailedRunSteps(_, _, eLogs, fSession) ⇒
              val fullLogs = (updatedLogs ++ eLogs :+ FailureLogInstruction(s"Eventually block did not complete in time", depth, Some(executionTime))) ++ logNonExecutedStep(nextSteps, depth)
              f.copy(logs = fullLogs, session = fSession)
          }
        }

      case c @ ConcurrentStart(factor, maxTime) ⇒
        val updatedLogs = logs :+ DefaultLogInstruction(c.title, depth)
        val concurrentSteps = findEnclosedSteps(c, steps.tail)

        if (concurrentSteps.isEmpty) {
          val innerLogs = logs ++ errorLogs(c.title, MalformedConcurrentBlock, depth, steps.tail)
          buildFailedRunSteps(steps, c, MalformedConcurrentBlock, innerLogs, session)
        } else {
          val start = System.nanoTime
          val f = Future.traverse(List.fill(factor)(concurrentSteps)) { steps ⇒
            Future { runSteps(steps, session, updatedLogs, depth + 1) }
          }

          val results = Try { Await.result(f, maxTime) } match {
            case Success(s) ⇒ s
            case Failure(e) ⇒ List(buildFailedRunSteps(steps, c, ConcurrentlyTimeout, updatedLogs, session))
          }

          val failedStepRun = results.collectFirst { case f @ FailedRunSteps(_, _, _, _) ⇒ f }
          val nextSteps = steps.tail.drop(concurrentSteps.size)
          failedStepRun.fold {
            val executionTime = Duration.fromNanos(System.nanoTime - start)
            val successStepsRun = results.collect { case s @ SuccessRunSteps(_, _) ⇒ s }
            val updatedSession = successStepsRun.head.session
            val updatedLogs = successStepsRun.head.logs :+ SuccessLogInstruction(s"Concurrently block with factor '$factor' succeeded", depth, Some(executionTime))
            runSteps(nextSteps, updatedSession, updatedLogs, depth)
          } { f ⇒
            f.copy(logs = (f.logs :+ FailureLogInstruction(s"Concurrently block failed", depth)) ++ logNonExecutedStep(nextSteps, depth))
          }
        }

      case w @ WithinStart(maxDuration) ⇒
        val updatedLogs = logs :+ DefaultLogInstruction(w.title, depth)
        val withinSteps = findEnclosedSteps(w, steps.tail)
        val (res, executionTime) = withDuration {
          runSteps(withinSteps, session, Vector.empty, depth + 1)
        }
        val nextSteps = steps.tail.drop(withinSteps.size)
        res match {
          case s @ SuccessRunSteps(sSession, sLogs) ⇒
            val successLogs = updatedLogs ++ sLogs
            if (executionTime.gt(maxDuration)) {
              val fullLogs = (successLogs :+ FailureLogInstruction(s"Within block did not complete in time", depth, Some(executionTime))) ++ logNonExecutedStep(nextSteps, depth)
              buildFailedRunSteps(steps, steps.last, WithinBlockSucceedAfterMaxDuration, fullLogs, sSession)
            } else {
              val fullLogs = successLogs :+ SuccessLogInstruction(s"Within block succeeded", depth, Some(executionTime))
              runSteps(nextSteps, sSession, fullLogs, depth)
            }
          case f @ FailedRunSteps(_, _, eLogs, fSession) ⇒
            // Failure of the nested steps have a higher priority
            val fullLogs = updatedLogs ++ eLogs ++ logNonExecutedStep(nextSteps, depth)
            f.copy(logs = fullLogs, session = fSession)
        }

      case r @ RepeatStart(occurence) ⇒
        val updatedLogs = logs :+ DefaultLogInstruction(r.title, depth)
        val repeatSteps = findEnclosedSteps(r, steps.tail)
        val (repeatRes, executionTime) = withDuration {
          // Session not propagated through repeat calls
          Vector.range(0, occurence).map(i ⇒ runSteps(repeatSteps, session, Vector.empty, depth + 1))
        }
        val nextSteps = steps.tail.drop(repeatSteps.size)
        if (repeatRes.forall(_.isSuccess == true)) {
          val fullLogs = updatedLogs ++ repeatRes.flatMap(_.logs) :+ SuccessLogInstruction(s"Repeat block with occurence $occurence succeeded", depth, Some(executionTime))
          runSteps(nextSteps, repeatRes.last.session, fullLogs, depth)
        } else {
          val fullLogs = updatedLogs ++ repeatRes.flatMap(_.logs) :+ FailureLogInstruction(s"Repeat block with occurence $occurence failed", depth, Some(executionTime))
          buildFailedRunSteps(steps, steps.last, RepeatBlockContainFailedSteps, fullLogs, repeatRes.last.session)
        }

      case r @ RepeatDuringStart(duration) ⇒
        val updatedLogs = logs :+ DefaultLogInstruction(r.title, depth)
        val repeatSteps = findEnclosedSteps(r, steps.tail)

        val (repeatRes, executionTime) = withDuration {
          repeatStepsDuring(repeatSteps, session, duration, Vector.empty, depth + 1)
        }

        val nextSteps = steps.tail.drop(repeatSteps.size)
        if (repeatRes.isSuccess) {
          val fullLogs = (updatedLogs ++ repeatRes.logs) :+ SuccessLogInstruction(s"Repeat block during $duration succeeded", depth, Some(executionTime))
          runSteps(nextSteps, repeatRes.session, fullLogs, depth)
        } else {
          val fullLogs = (updatedLogs ++ repeatRes.logs) :+ FailureLogInstruction(s"Repeat block during $duration failed", depth, Some(executionTime))
          buildFailedRunSteps(steps, steps.last, RepeatDuringBlockContainFailedSteps, fullLogs, repeatRes.session)
        }

      case r @ RetryMaxStart(limit) ⇒
        val updatedLogs = logs :+ DefaultLogInstruction(r.title, depth)
        val retrySteps = findEnclosedSteps(r, steps.tail)

        val (repeatRes, executionTime) = withDuration {
          retryMaxSteps(retrySteps, session, limit, Vector.empty, depth + 1)
        }

        val nextSteps = steps.tail.drop(retrySteps.size)
        if (repeatRes.isSuccess) {
          val fullLogs = (updatedLogs ++ repeatRes.logs) :+ SuccessLogInstruction(s"RetryMax block with limit $limit succeeded", depth, Some(executionTime))
          runSteps(nextSteps, repeatRes.session, fullLogs, depth)
        } else {
          val fullLogs = (updatedLogs ++ repeatRes.logs) :+ FailureLogInstruction(s"RetryMax block with limit $limit failed", depth, Some(executionTime))
          buildFailedRunSteps(steps, steps.last, RetryMaxBlockReachedLimit, fullLogs, repeatRes.session)
        }

      // The end blocks are only used for nesting detection
      case RepeatStop | WithinStop | EventuallyStop | ConcurrentStop | RepeatDuringStop | RetryMaxStop ⇒
        runSteps(steps.tail, session, logs, depth)

      case a @ AssertStep(title, toAssertion, negate, show) ⇒
        val res = Xor.catchNonFatal(toAssertion(session))
          .leftMap(toCornichonError)
          .flatMap { assertion ⇒
            runStepPredicate(negate, session, assertion)
          }
        buildStepReport(steps, session, logs, res, title, depth, show)

      case e @ EffectStep(title, effect, show) ⇒
        val (res, executionTime) = withDuration {
          Xor.catchNonFatal(effect(session)).leftMap(toCornichonError)
        }
        buildStepReport(steps, session, logs, res, title, depth, show, Some(executionTime))

    }

  private def buildStepReport(steps: Vector[Step], session: Session, logs: Vector[LogInstruction], res: Xor[CornichonError, Session], title: String, depth: Int, show: Boolean, duration: Option[Duration] = None) =
    res match {
      case Xor.Left(e) ⇒
        val updatedLogs = logs ++ errorLogs(title, e, depth, steps.tail)
        buildFailedRunSteps(steps, steps.head, e, updatedLogs, session)

      case Xor.Right(currentSession) ⇒
        val updatedLogs = if (show) logs :+ SuccessLogInstruction(title, depth, duration) else logs
        runSteps(steps.tail, currentSession, updatedLogs, depth)
    }

  private[cornichon] def toCornichonError(exception: Throwable): CornichonError = exception match {
    case ce: CornichonError ⇒ ce
    case _                  ⇒ StepExecutionError(exception)
  }

  private[cornichon] def runStepPredicate[A](negateStep: Boolean, newSession: Session, stepAssertion: StepAssertion[A]): Xor[CornichonError, Session] = {
    val succeedAsExpected = stepAssertion.isSuccess && !negateStep
    val failedAsExpected = !stepAssertion.isSuccess && negateStep

    if (succeedAsExpected || failedAsExpected) right(newSession)
    else
      stepAssertion match {
        case SimpleStepAssertion(expected, actual) ⇒
          left(StepAssertionError(expected, actual, negateStep))
        case DetailedStepAssertion(expected, actual, details) ⇒
          left(DetailedStepAssertionError(actual, details))
      }
  }

  //TODO remove duplication
  private[cornichon] def findEnclosedSteps(openingStep: Step, steps: Vector[Step]): Vector[Step] = {
    def findLastEnclosedIndex(openingStep: Step, steps: Vector[Step], index: Int, depth: Int): Int = {
      steps.headOption.fold(index) { head ⇒
        openingStep match {
          case ConcurrentStart(_, _) ⇒
            head match {
              case ConcurrentStop if depth == 0 ⇒
                index
              case ConcurrentStop ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth - 1)
              case ConcurrentStart(_, _) ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth + 1)
              case _ ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth)
            }
          case EventuallyStart(_) ⇒
            head match {
              case EventuallyStop if depth == 0 ⇒
                index
              case EventuallyStop ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth - 1)
              case EventuallyStart(_) ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth + 1)
              case _ ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth)
            }
          case WithinStart(_) ⇒
            head match {
              case WithinStop if depth == 0 ⇒
                index
              case WithinStop ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth - 1)
              case WithinStart(_) ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth + 1)
              case _ ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth)
            }
          case RepeatStart(_) ⇒
            head match {
              case RepeatStop if depth == 0 ⇒
                index
              case RepeatStop ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth - 1)
              case RepeatStart(_) ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth + 1)
              case _ ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth)
            }
          case RepeatDuringStart(_) ⇒
            head match {
              case RepeatDuringStop if depth == 0 ⇒
                index
              case RepeatDuringStop ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth - 1)
              case RepeatDuringStart(_) ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth + 1)
              case _ ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth)
            }
          case RetryMaxStart(_) ⇒
            head match {
              case RetryMaxStop if depth == 0 ⇒
                index
              case RetryMaxStop ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth - 1)
              case RetryMaxStart(_) ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth + 1)
              case _ ⇒
                findLastEnclosedIndex(openingStep, steps.tail, index + 1, depth)
            }
          case _ ⇒ index
        }
      }
    }

    val closingIndex = findLastEnclosedIndex(openingStep, steps, index = 0, depth = 0)
    if (closingIndex == 0) Vector.empty else steps.take(closingIndex)
  }

  @tailrec
  private[cornichon] final def retryEventuallySteps(steps: Vector[Step], session: Session, conf: EventuallyConf, accLogs: Vector[LogInstruction], depth: Int): StepsReport = {
    val (res, executionTime) = withDuration {
      runSteps(steps, session, Vector.empty, depth)
    }
    val remainingTime = conf.maxTime - executionTime
    res match {
      case s @ SuccessRunSteps(successSession, sLogs) ⇒
        val runLogs = accLogs ++ sLogs
        if (remainingTime.gt(Duration.Zero)) s.copy(logs = runLogs)
        else buildFailedRunSteps(steps, steps.last, EventuallyBlockSucceedAfterMaxDuration, runLogs, successSession)
      case f @ FailedRunSteps(failed, _, fLogs, fSession) ⇒
        val updatedLogs = accLogs ++ fLogs
        if ((remainingTime - conf.interval).gt(Duration.Zero)) {
          Thread.sleep(conf.interval.toMillis)
          retryEventuallySteps(steps, session, conf.consume(executionTime + conf.interval), updatedLogs, depth)
        } else f.copy(logs = updatedLogs, session = fSession)
    }
  }

  @tailrec
  private[cornichon] final def repeatStepsDuring(steps: Vector[Step], session: Session, duration: Duration, accLogs: Vector[LogInstruction], depth: Int): StepsReport = {
    val (res, executionTime) = withDuration {
      runSteps(steps, session, Vector.empty, depth)
    }
    val remainingTime = duration - executionTime
    if (remainingTime.gt(Duration.Zero))
      repeatStepsDuring(steps, session, remainingTime, accLogs ++ res.logs, depth)
    else
      res match {
        case s @ SuccessRunSteps(sSession, sLogs)      ⇒ s.copy(logs = accLogs ++ sLogs)
        case f @ FailedRunSteps(_, _, eLogs, fSession) ⇒ f.copy(logs = accLogs ++ eLogs)
      }
  }

  @tailrec
  private[cornichon] final def retryMaxSteps(steps: Vector[Step], session: Session, limit: Int, accLogs: Vector[LogInstruction], depth: Int): StepsReport = {
    runSteps(steps, session, Vector.empty, depth) match {
      case s @ SuccessRunSteps(sSession, sLogs) ⇒ s.copy(logs = accLogs ++ sLogs)
      case f @ FailedRunSteps(_, _, eLogs, fSession) ⇒
        if (limit > 0)
          retryMaxSteps(steps, session, limit - 1, accLogs ++ eLogs, depth)
        else
          f.copy(logs = accLogs ++ eLogs)
    }
  }

  private[cornichon] def logNonExecutedStep(steps: Seq[Step], depth: Int): Seq[LogInstruction] =
    steps.collect {
      case a @ AssertStep(_, _, _, true) ⇒ a
      case e @ EffectStep(_, _, true)    ⇒ e
    }.map { step ⇒
      InfoLogInstruction(step.title, depth)
    }

  private[cornichon] def errorLogs(title: String, error: CornichonError, depth: Int, remainingSteps: Vector[Step]) = {
    val logStepErrorResult = Vector(FailureLogInstruction(s"$title *** FAILED ***", depth)) ++ error.msg.split('\n').map { m ⇒
      FailureLogInstruction(m, depth)
    }
    logStepErrorResult ++ logNonExecutedStep(remainingSteps, depth)
  }

  private[cornichon] def buildFailedRunSteps(steps: Vector[Step], currentStep: Step, e: CornichonError, logs: Vector[LogInstruction], session: Session): FailedRunSteps = {
    val failedStep = FailedStep(currentStep, e)
    val notExecutedStep = steps.tail.collect {
      case AssertStep(t, _, _, true) ⇒ t
      case EffectStep(t, _, true)    ⇒ t
    }
    FailedRunSteps(failedStep, notExecutedStep, logs, session)
  }

  private[cornichon] def withDuration[A](fct: ⇒ A): (A, Duration) = {
    val now = System.nanoTime
    val res = fct
    val executionTime = Duration.fromNanos(System.nanoTime - now)
    (res, executionTime)
  }
}