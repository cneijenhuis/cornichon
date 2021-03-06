package com.github.agourlay.cornichon.core

import cats.instances.list._
import cats.instances.vector._

import cats.kernel.Monoid
import cats.syntax.monoid._

case class RunState(
    remainingSteps: List[Step],
    session: Session,
    logs: Vector[LogInstruction],
    depth: Int,
    cleanupSteps: List[Step]
) {

  lazy val goDeeper = copy(depth = depth + 1)

  lazy val resetLogs = copy(logs = Vector.empty)

  def withSteps(steps: List[Step]) = copy(remainingSteps = steps)
  // Helper fct to set remaining steps, go deeper and reset logs
  def forNestedSteps(steps: List[Step]) = copy(remainingSteps = steps, depth = depth + 1, logs = Vector.empty)

  def withSession(s: Session) = copy(session = s)
  def addToSession(tuples: Seq[(String, String)]) = withSession(session.addValuesUnsafe(tuples: _*))
  def addToSession(key: String, value: String) = withSession(session.addValueUnsafe(key, value))
  def mergeSessions(other: Session) = copy(session = session.combine(other))

  def withLogs(logs: Vector[LogInstruction]) = copy(logs = logs)
  def withLog(log: LogInstruction) = copy(logs = Vector(log))

  // Vector concat. is not great, maybe change logs data structure
  def appendLogs(add: Vector[LogInstruction]) = copy(logs = logs ++ add)
  def appendLogsFrom(fromRun: RunState) = appendLogs(fromRun.logs)
  def appendLog(add: LogInstruction) = copy(logs = logs :+ add)

  def prependSteps(prepend: List[Step]) = copy(remainingSteps = prepend ++ remainingSteps)

}

object RunState {

  implicit val monoidRunState = new Monoid[RunState] {
    def empty: RunState = RunState(Nil, Session.newEmpty, Vector.empty, 1, Nil)
    def combine(x: RunState, y: RunState): RunState = x.copy(
      remainingSteps = x.remainingSteps.combine(y.remainingSteps),
      session = x.session.combine(y.session),
      logs = x.logs.combine(y.logs)
    )
  }
}