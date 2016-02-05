/*
 * The MIT License
 *
 * Copyright (c) 2015 Fulcrum Genomics LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package dagr.core.execsystem

import dagr.core.tasksystem._
import dagr.core.util._
import org.scalatest._

object TaskManagerTest {

  trait TryThreeTimesTask extends MultipleRetryTask {
    override def maxNumIterations: Int = 3
  }

  trait SucceedOnTheThirdTry extends TryThreeTimesTask {
    override def retry(taskInfo: TaskExecutionInfo, failedOnComplete: Boolean): Option[Task] = {
      if (maxNumIterations <= taskInfo.attemptIndex) Some(ShellCommand("exit 0") withName "exit 0")
      else Some(taskInfo.task)
    }
  }
}

class TaskManagerTest extends UnitSpec with PrivateMethodTester with OptionValues with LazyLogging with BeforeAndAfterAll {

  override def beforeAll(): Unit = Logger.level = LogLevel.Fatal
  override def afterAll(): Unit = Logger.level = LogLevel.Info

  def getDefaultTaskManager: TaskManager = new TaskManager(taskManagerResources = TaskManagerResources.infinite, scriptsDirectory = None)

  private def runSchedulerOnce(taskManager:TaskManager,
                               tasksToScheduleContains: List[Task],
                               runningTasksContains: List[Task],
                               completedTasksContains: List[Task],
                               failedAreCompleted: Boolean = true) = {
    val (readyTasks, tasksToSchedule, runningTasks, completedTasks) = taskManager.runSchedulerOnce()
    tasksToScheduleContains.foreach(task => tasksToSchedule should contain(task))
    runningTasksContains.foreach(task => runningTasks should contain(task))
    completedTasksContains.foreach(task => completedTasks should contain(task))
  }

  // Expects that the task will complete (either successfully or with failure) on the nth try
  def tryTaskNTimes(taskManager:TaskManager, task: UnitTask, numTimes: Int, taskIsDoneFinally: Boolean, failedAreCompletedFinally: Boolean = true) = {

    // add the task
    taskManager.addTask(task)
    TaskStatus.isTaskDone(taskManager.getTaskStatus(task).get) should be(false)

    // run the task, and do not report failed tasks as completed
    for (i <- 1 to numTimes) {
      logger.debug("running the scheduler the " + i + "th time")
      runSchedulerOnce(taskManager = taskManager, tasksToScheduleContains = List[Task](task), runningTasksContains = Nil, completedTasksContains = Nil, failedAreCompleted = false)
      TaskStatus.isTaskDone(taskStatus = taskManager.getTaskStatus(task).get, failedIsDone = false) should be(false)
    }

    // run the task a fourth time, but set that any failed tasks should be assumed to be completed
    logger.debug("running the scheduler the last (" + (numTimes + 1) + "th) time")
    runSchedulerOnce(taskManager = taskManager, tasksToScheduleContains = Nil, runningTasksContains = Nil, completedTasksContains = List[Task](task), failedAreCompleted = failedAreCompletedFinally)
    TaskStatus.isTaskDone(taskStatus = taskManager.getTaskStatus(task).get, failedIsDone = failedAreCompletedFinally) should be(taskIsDoneFinally)
  }

  "TaskManager" should "not overwrite an existing task when adding a task, or throw an IllegalArgumentException when ignoreExists is false" in {
    val task: UnitTask = new ShellCommand("exit", "0") withName "exit 0" requires ResourceSet.empty
    val taskManager: TaskManager = getDefaultTaskManager
    taskManager.addTasks(tasks=Seq(task, task), parent=None, ignoreExists=true) shouldBe List(0, 0)
    an[IllegalArgumentException] should be thrownBy taskManager.addTask(task=task, parent=None, ignoreExists=false)

  }

  it should "get a task by its graph node only if the graph node is being tracked by TaskManagerState" in {
    val task: UnitTask = new ShellCommand("exit", "0") withName "exit 0" requires ResourceSet.empty
    val taskManager: TaskManager = getDefaultTaskManager
    taskManager.addTask(task=task, parent=None, ignoreExists=true) shouldBe 0
    val node = taskManager.getGraphNode(task).get
    taskManager.getTask(node).foreach(t => t shouldBe task)
    val unknownNode = new GraphNode(taskId=1, task=task)
    taskManager.getTask(unknownNode) shouldBe 'empty
  }

  it should "get a task info by its graph node only if the graph node is being tracked by TaskManagerState" in {
    val task: UnitTask = new ShellCommand("exit", "0") withName "exit 0" requires ResourceSet.empty
    val taskManager: TaskManager = getDefaultTaskManager
    taskManager.addTask(task=task, parent=None, ignoreExists=true) shouldBe 0
    val taskInfo = taskManager.getTaskExecutionInfo(task).get
    val node = taskManager.getGraphNode(task).get
    taskManager.getTaskExecutionInfo(node).foreach(info => info shouldBe taskInfo)
    val unknownNode = new GraphNode(taskId=1, task=task)
    taskManager.getTaskExecutionInfo(unknownNode) shouldBe 'empty
  }

  it should "get the task status for only tracked tasks" in {
    val task: UnitTask = new ShellCommand("exit", "0") withName "exit 0" requires ResourceSet.empty
    val taskManager: TaskManager = getDefaultTaskManager
    taskManager.addTask(task=task, parent=None, ignoreExists=true) shouldBe 0
    taskManager.getTaskStatus(0) shouldBe 'defined
    taskManager.getTaskStatus(1) shouldBe 'empty
  }

  it should "get the graph node state for only tracked tasks" in {
    val task: UnitTask = new ShellCommand("exit", "0") withName "exit 0" requires ResourceSet.empty
    val taskManager: TaskManager = getDefaultTaskManager
    taskManager.addTask(task=task, parent=None, ignoreExists=true) shouldBe 0
    taskManager.getGraphNodeState(0) shouldBe 'defined
    taskManager.getGraphNodeState(1) shouldBe 'empty
  }

  // ******************************************
  // Simple Tasks
  // ******************************************

  it should "run a simple task" in {
    val task: UnitTask = new ShellCommand("exit", "0") withName "exit 0" requires ResourceSet.empty
    val taskManager: TaskManager = getDefaultTaskManager

    tryTaskNTimes(taskManager = taskManager, task = task, numTimes = 1, taskIsDoneFinally = true, failedAreCompletedFinally = true)
  }

  it should "run a simple task that fails but we allow it" in {
    val task: UnitTask = new ShellCommand("exit", "1") withName "exit 1"
    val taskManager: TaskManager = getDefaultTaskManager

    tryTaskNTimes(taskManager = taskManager, task = task, numTimes = 1, taskIsDoneFinally = true, failedAreCompletedFinally = true)
  }

  it should "run a simple task that fails and is never done" in {
    val task: UnitTask = new ShellCommand("exit", "1") withName "exit 1"
    val taskManager: TaskManager = getDefaultTaskManager

    tryTaskNTimes(taskManager = taskManager, task = task, numTimes = 1, taskIsDoneFinally = false, failedAreCompletedFinally = false)
  }

  it should "retry a task three times that ends up failed" in {
    val task: UnitTask = new ShellCommand("exit 1") with TaskManagerTest.TryThreeTimesTask withName "retry three times"
    val taskManager: TaskManager = getDefaultTaskManager

    // try it three times, and it will keep failing, and not omit a task at the last attempt
    tryTaskNTimes(taskManager = taskManager, task = task, numTimes = 3, taskIsDoneFinally = true, failedAreCompletedFinally = true)
  }

  it should "retry a task three times and succeed on the last try" in {
    val task: UnitTask = new ShellCommand("exit 1") with TaskManagerTest.SucceedOnTheThirdTry withName "succeed on the third try"
    val taskManager: TaskManager = getDefaultTaskManager

    // try it three times, and it will keep failing, and not omit a task at the last attempt
    tryTaskNTimes(taskManager = taskManager, task = task, numTimes = 3, taskIsDoneFinally = false, failedAreCompletedFinally = true)
  }

  def runSimpleEndToEnd(task: UnitTask = new ShellCommand("exit", "0") withName "exit 0", simulate: Boolean): Unit = {
    val map: BiMap[Task, TaskExecutionInfo] = TaskManager.run(
      task                 = task,
      sleepMilliseconds    = 10,
      taskManagerResources = Some(TaskManagerResources.infinite),
      scriptsDirectory     = None,
      simulate             = simulate)

    map.containsKey(task) should be(true)

    val taskInfo: TaskExecutionInfo = map.getValue(task).get
    taskInfo.id should be(0)
    taskInfo.task should be(task)
    taskInfo.status should be(TaskStatus.SUCCEEDED)
    taskInfo.attemptIndex should be(1)
  }

  it should "run a simple task end-to-end" in {
    runSimpleEndToEnd(simulate = false)
  }

  it should "run a simple task end-to-end in simulate mode" in {
    runSimpleEndToEnd(simulate = true)
  }

  it should "run a simple task that fails but we allow in simulate mode" in {
    val task: UnitTask = new ShellCommand("exit", "1") withName "exit 1"
    runSimpleEndToEnd(task = task, simulate = true)
  }

  it should "terminate all running jobs before returning from runAllTasks" in {
    val longTask: UnitTask = new ShellCommand("sleep", "1000") withName "sleep 1000"
    val failedTask: UnitTask = new ShellCommand("exit", "1") withName "exit 1"

    val taskManager: TaskManager = getDefaultTaskManager
    taskManager.addTasks(longTask, failedTask)
    taskManager.runAllTasks(sleepMilliseconds=1, timeout=1)
    taskManager.getTaskStatus(failedTask).value should be(TaskStatus.FAILED_COMMAND)
    taskManager.getGraphNodeState(failedTask).value should be(GraphNodeState.COMPLETED)
    taskManager.getTaskStatus(longTask).value should be(TaskStatus.FAILED_COMMAND)
    taskManager.getGraphNodeState(longTask).value should be(GraphNodeState.COMPLETED)
  }

  // ******************************************
  // Task retry and replacement
  // *****************************************
  protected class TestPipeline(original: Task) extends Pipeline {
    withName("TestWorkflow")
    override def build(): Unit = {
      val first: UnitTask = new ShellCommand("exit", "0") withName "first exit 0"
      val third: UnitTask = new ShellCommand("exit", "0") withName "third exit 0"
      // dependencies: first <- original <- third
      root ==> first ==> original ==> third
    }
  }

  def doTryAgainRun(original: Task,
                    originalTaskStatus: TaskStatus.Value,
                    originalGraphNodeState: GraphNodeState.Value,
                    taskManager: TaskManager): Unit = {
    // run it once, make sure tasks that are failed are not marked as completed
    taskManager.runAllTasks(sleepMilliseconds = 10)

    // the task identifier for 'original' should now be found
    val originalTaskId: BigInt = taskManager.getTaskId(original).value

    // check the original task status
    val taskInfo: TaskExecutionInfo = taskManager.getTaskExecutionInfo(originalTaskId).value
    taskInfo.status should be(originalTaskStatus)

    // check the original graph node state
    val state: GraphNodeState.Value = taskManager.getGraphNodeState(originalTaskId).value
    state should be(originalGraphNodeState)
  }

  def doTryAgain(original: Task,
                 replacement: Task,
                 replaceTask: Boolean = true,
                 taskManager: TaskManager) {

    val (originalTaskStatus, originalGraphNodeState) = replaceTask match {
      case true => (TaskStatus.UNKNOWN, GraphNodeState.NO_PREDECESSORS)
      case false => (TaskStatus.FAILED_COMMAND, GraphNodeState.COMPLETED)
    }

    val wf: TestPipeline = new TestPipeline(original)
    taskManager.addTask(wf) should be(0)

    // run and check
    doTryAgainRun(original = original, originalTaskStatus = originalTaskStatus, originalGraphNodeState = originalGraphNodeState, taskManager = taskManager)

    if (replaceTask) {
      // replace
      logger.debug("*** REPLACING TASK ***")
      taskManager.replaceTask(original = original, replacement = replacement)
    }
    else {
      // resubmit...
      logger.debug("*** RESUBMIT TASK ***")
      taskManager.resubmitTask(original)
    }

    // run and check again
    doTryAgainRun(original = if (replaceTask) replacement else original,
                  originalTaskStatus = TaskStatus.SUCCEEDED,
                  originalGraphNodeState = GraphNodeState.COMPLETED,
                  taskManager = taskManager)
  }

  it should "replace a task that could not be scheduled due to OOM and re-run with less memory to completion" in {
    val original = new ShellCommand("exit 0").requires(memory=Memory("2G")) withName "Too much memory"
    val replacement = new ShellCommand("exit 0").requires(memory=Memory("1G")) withName "Just enough memory"
    val taskManager: TaskManager = new TaskManager(taskManagerResources = new TaskManagerResources(Cores(1), Memory("1G"), Memory(0)), scriptsDirectory = None)

    // just in case
    original.resources.memory.bytes should be(Resource.parseSizeToBytes("2G"))
    replacement.resources.memory.bytes should be(Resource.parseSizeToBytes("1G"))
    taskManager.getTaskManagerResources.systemMemory.value should be(Resource.parseSizeToBytes("1G"))

    doTryAgain(original = original, replacement = replacement, replaceTask = true, taskManager = taskManager)
  }

  // Really simple class that when retry gets called, it changes its args, but doesn't asked to be retried!
  class RetryMutatorTask extends ProcessTask with FixedResources {
    private var fail = true
    override def args = List("exit", if (fail) "1"  else "0")
    override def retry(taskInfo: TaskExecutionInfo, failedOnComplete: Boolean): Option[Task] = {
      fail = false
      None
    }
  }

  it should "resubmit a task that failed and re-run to completion" in {
    val original: UnitTask = new RetryMutatorTask
    val taskManager: TaskManager = getDefaultTaskManager

    doTryAgain(original = original, replacement = null, replaceTask = false, taskManager = taskManager)
  }

  it should "return false when replacing an un-tracked task" in {
    val task: UnitTask = new ShellCommand("exit 0") withName "Blah"
    val taskManager: TaskManager = getDefaultTaskManager
    taskManager.replaceTask(task, null) should be(false)
  }

  it should "return false when resubmitting an un-tracked task" in {
    val task: UnitTask = new ShellCommand("exit 0") withName "Blah"
    val taskManager: TaskManager = getDefaultTaskManager
    taskManager.resubmitTask(task) should be(false)
  }

  // ******************************************
  // onComplete success and failure
  // ******************************************

  // the onComplete method returns false until the retry method is called
  private trait FailOnCompleteTask extends UnitTask {
    private var onCompleteValue = false

    override def onComplete(exitCode: Int): Boolean = onCompleteValue

    override def retry(taskInfo: TaskExecutionInfo, failedOnComplete: Boolean): Option[Task] = {
      onCompleteValue = true
      Some(this)
    }
  }

  // the onComplete method modifies the arguments for this task, and in its first call is false, otherwise true
  private class FailOnCompleteAndClearArgsTask(name: String, originalArgs: List[String], newArgs: List[String] = List("exit 0"))
    extends ProcessTask with FixedResources {
    withName(name)
    requires(ResourceSet.empty)
    private var onCompleteValue = false
    override def args = if (!onCompleteValue) originalArgs else newArgs

    override def onComplete(exitCode: Int): Boolean = {
      val previousOnCompleteValue: Boolean = onCompleteValue
      onCompleteValue = true
      previousOnCompleteValue
    }

    override def retry(taskInfo: TaskExecutionInfo, failedOnComplete: Boolean): Option[Task] = Some(this)
  }

  // The first time this task is run, it has exit code 1 and fails onComplete.
  // The second time this task is run, it has exit code 0 and fails onComplete
  // The third time and subsequent time this task is run, it has exit code 0 and succeed onComplete.
  private class MultiFailTask(name: String) extends ProcessTask with FixedResources {
    withName(name)
    private var onCompleteValue = false
    private var attemptIndex = 0
    override def args = if (attemptIndex == 0) List("exit 1") else List("exit 0")


    override def onComplete(exitCode: Int): Boolean = {
      val previousOnCompleteValue: Boolean = onCompleteValue
      // first attempt
      if (1 == attemptIndex) {
        onCompleteValue = true
      }
      previousOnCompleteValue
    }

    override def retry(taskInfo: TaskExecutionInfo, failedOnComplete: Boolean): Option[Task] = {
      attemptIndex = taskInfo.attemptIndex
      Some(this)
    }
  }

  def runTasksMultipleTimes(taskManager: TaskManager, task: UnitTask, statuses: List[(TaskStatus.Value, Int, Boolean)]): Unit = {
    // add the task
    taskManager.addTask(task)
    TaskStatus.isTaskDone(taskManager.getTaskStatus(task).get) should be(false)
    val taskId: BigInt = taskManager.getTaskId(task).value

    for ((taskStatus, exitCode, onCompleteSuccessful) <- statuses) {
      // run the task once
      runSchedulerOnce(taskManager = taskManager, tasksToScheduleContains = List[Task](task), runningTasksContains = Nil, completedTasksContains = Nil, failedAreCompleted = false)

      // we need to check the status of the task after it has completed but before it has been retried, so do this part manually.
      val taskRunner: TaskRunner = taskManager invokePrivate PrivateMethod[TaskRunner]('taskRunner)()
      val processCompletedTask = PrivateMethod[Unit]('processCompletedTask)
      val completedTasks = taskRunner.getCompletedTasks(failedAreCompleted = false) // get the complete tasks from the task runner

      // now we can check status, exit code, and onComplete success
      completedTasks.get(taskId).value._1 should be(exitCode)
      completedTasks.get(taskId).value._2 should be(onCompleteSuccessful)
      taskManager.getTaskStatus(task).value should be(taskStatus)

      // retry and update the completed tasks
      completedTasks.keys.foreach(tid => {
        taskManager invokePrivate processCompletedTask(tid, true)
      })
    }
  }

  it should "run a task that fails its onComplete method, is retried, where it modifies the onComplete method return value, and succeeds" in {
    val task: UnitTask = new ShellCommand("exit 0") with FailOnCompleteTask withName "Dummy"
    val taskManager: TaskManager = getDefaultTaskManager
    runTasksMultipleTimes(taskManager = taskManager, task = task, statuses = List((TaskStatus.FAILED_ON_COMPLETE, 0, false), (TaskStatus.SUCCEEDED, 0, true)))
  }

  it should "run a task that fails its onComplete method, whereby it changes its args to empty, and succeeds" in {
    val task: UnitTask = new FailOnCompleteAndClearArgsTask(name = "Dummy", originalArgs = List("exit 0"))
    val taskManager: TaskManager = getDefaultTaskManager
    runTasksMultipleTimes(taskManager = taskManager, task = task, statuses = List((TaskStatus.FAILED_ON_COMPLETE, 0, false), (TaskStatus.SUCCEEDED, 0, true)))
  }

  it should "run a task, that its onComplete method mutates its args and return value based on the attempt index" in {
    val task: UnitTask = new MultiFailTask(name = "Dummy")
    val taskManager: TaskManager = getDefaultTaskManager

    runTasksMultipleTimes(taskManager = taskManager,
      task = task,
      statuses = List(
        (TaskStatus.FAILED_COMMAND, 1, false),
        (TaskStatus.FAILED_ON_COMPLETE, 0, false),
        (TaskStatus.SUCCEEDED, 0, true)
      )
    )
  }

  // ********************************************
  // in Jvm tasks
  // ********************************************

  private class SimpleInJvmTask(exitCode: Int) extends InJvmTask with FixedResources {
    override def inJvmMethod(): Int = exitCode
  }

  private class SimpleRetryInJvmTask extends InJvmTask with FixedResources {
    private var exitCode = 1

    override def inJvmMethod(): Int = exitCode

    override def retry(taskInfo: TaskExecutionInfo, failedOnComplete: Boolean): Option[Task] = {
      exitCode = 0
      Some(this)
    }
  }

  private class SimpleRetryOnCompleteInJvmTask extends InJvmTask with FixedResources {
    private var exitCode = 1

    override def inJvmMethod(): Int = exitCode

    override def retry(taskInfo: TaskExecutionInfo, failedOnComplete: Boolean): Option[Task] = {
      Some(this)
    }

    override def onComplete(exitCode: Int): Boolean = {
      this.exitCode = 0
      true
    }
  }

  // The first time this task is run, it has exit code 1 and fails onComplete.
  // The second time this task is run, it has exit code 0 and fails onComplete
  // The third time and subsequent time this task is run, it has exit code 0 and succeed onComplete.
  private class MultiFailInJvmTask(name: String) extends InJvmTask with FixedResources {
    private var onCompleteValue = false
    private var attemptIndex = 0
    private var exitCode = 1

    override def inJvmMethod(): Int = exitCode

    override def onComplete(exitCode: Int): Boolean = {
      val previousOnCompleteValue: Boolean = onCompleteValue
      // first attempt
      if (0 == attemptIndex) {
        // the first call to onComplete
        this.exitCode = 0
      }
      else if (1 == attemptIndex) {
        onCompleteValue = true
      }
      previousOnCompleteValue
    }

    override def retry(taskInfo: TaskExecutionInfo, failedOnComplete: Boolean): Option[Task] = {
      attemptIndex = taskInfo.attemptIndex
      Some(this)
    }
  }

  it should "run an in Jvm task that succeeds" in {
    runSimpleEndToEnd(task = new SimpleInJvmTask(exitCode = 0), simulate = false)
  }

  it should "run an in Jvm that fails" in {
    val taskManager: TaskManager = getDefaultTaskManager
    tryTaskNTimes(taskManager = taskManager, task = new SimpleInJvmTask(exitCode = 1), numTimes = 1, taskIsDoneFinally = true, failedAreCompletedFinally = true)
  }

  it should "run inJvm tasks that can retry" in {
    val taskManager: TaskManager = getDefaultTaskManager
    tryTaskNTimes(taskManager = taskManager, task = new SimpleRetryInJvmTask, numTimes = 2, taskIsDoneFinally = true, failedAreCompletedFinally = true)
    tryTaskNTimes(taskManager = taskManager, task = new SimpleRetryOnCompleteInJvmTask, numTimes = 2, taskIsDoneFinally = true, failedAreCompletedFinally = true)
  }

  it should "run an in Jvm task, that its onComplete method mutates its args and return value based on the attempt index" in {
    val task: UnitTask = new MultiFailInJvmTask(name = "Dummy")
    val taskManager: TaskManager = getDefaultTaskManager

    runTasksMultipleTimes(taskManager = taskManager,
      task = task,
      statuses = List(
        (TaskStatus.FAILED_COMMAND, 1, false),
        (TaskStatus.FAILED_ON_COMPLETE, 0, false),
        (TaskStatus.SUCCEEDED, 0, true)
      )
    )
  }

  // *************************************************
  // Orphans: tasks whose predecessors have not been added
  // *************************************************

  it should "run a task for which its predecessor has not been added and put it into an orphan state" in {
    val predecessor: UnitTask = new ShellCommand("exit", "0") withName "predecessor"
    val successor: UnitTask = new ShellCommand("exit", "0") withName "successor"
    val taskManager: TaskManager = getDefaultTaskManager
    predecessor ==> successor
    taskManager.addTask(successor)
    taskManager.getGraphNodeState(successor).value should be(GraphNodeState.ORPHAN)
    taskManager.runSchedulerOnce()
    taskManager.getGraphNodeState(successor).value should be(GraphNodeState.ORPHAN)
  }

  it should "should resolve an orphan when its predecessor is added" in {
    val predecessor: UnitTask = new ShellCommand("exit", "0") withName "predecessor"
    val successor: UnitTask = new ShellCommand("exit", "0") withName "successor"
    val taskManager: TaskManager = getDefaultTaskManager
    predecessor ==> successor
    taskManager.addTask(successor)
    taskManager.getGraphNodeState(successor).value should be(GraphNodeState.ORPHAN)
    taskManager.addTask(predecessor)
    taskManager.getGraphNodeState(predecessor).value should be(GraphNodeState.PREDECESSORS_AND_UNEXPANDED)
    taskManager.runSchedulerOnce()
    taskManager.getGraphNodeState(successor).value should be(GraphNodeState.PREDECESSORS_AND_UNEXPANDED)
    taskManager.getGraphNodeState(predecessor).value should be(GraphNodeState.RUNNING)
    taskManager.runSchedulerOnce()
    taskManager.getGraphNodeState(successor).value should be(GraphNodeState.RUNNING)
    taskManager.getGraphNodeState(predecessor).value should be(GraphNodeState.COMPLETED)
    taskManager.runSchedulerOnce()
    taskManager.getGraphNodeState(successor).value should be(GraphNodeState.COMPLETED)
    taskManager.getGraphNodeState(predecessor).value should be(GraphNodeState.COMPLETED)
  }

  it should "should resolve multiple orphans when their ancestor is added" in {
    val leftA: UnitTask = new ShellCommand("exit", "0") withName "leftA"
    val leftB: UnitTask = new ShellCommand("exit", "0") withName "leftB"
    val rightA: UnitTask = new ShellCommand("exit", "0") withName "leftA"
    val rightB: UnitTask = new ShellCommand("exit", "0") withName "leftB"
    val finalTask: UnitTask = new ShellCommand("exit", "0") withName "finalTask"
    val taskManager: TaskManager = getDefaultTaskManager

    // leftA <- leftB <- finalTask -> rightB -> rightA
    (leftB :: rightB) ==> finalTask
    leftA ==> leftB
    rightA ==> rightB

    // add all but the right A
    taskManager.addTasks(leftA, leftB, rightB, finalTask)

    // run it until we cannot run any more tasks
    taskManager.getGraphNodeState(rightA) should be ('empty)
    taskManager.getGraphNodeState(rightB).value should be(GraphNodeState.ORPHAN)
    taskManager.runSchedulerOnce()
    taskManager.getGraphNodeState(leftA).value should be(GraphNodeState.RUNNING)
    taskManager.runSchedulerOnce()
    taskManager.getGraphNodeState(leftA).value should be(GraphNodeState.COMPLETED)
    taskManager.getGraphNodeState(leftB).value should be(GraphNodeState.RUNNING)
    taskManager.runSchedulerOnce()
    taskManager.getGraphNodeState(leftB).value should be(GraphNodeState.COMPLETED)
    taskManager.getGraphNodeState(finalTask).value should be(GraphNodeState.PREDECESSORS_AND_UNEXPANDED)
    taskManager.getGraphNodeState(rightB).value should be(GraphNodeState.ORPHAN)

    // now add right A
    taskManager.addTask(rightA)
    taskManager.runSchedulerOnce()
    taskManager.getGraphNodeState(rightA).value should be(GraphNodeState.RUNNING)
    taskManager.getGraphNodeState(rightB).value should be(GraphNodeState.PREDECESSORS_AND_UNEXPANDED)
    taskManager.runSchedulerOnce()
    taskManager.getGraphNodeState(rightA).value should be(GraphNodeState.COMPLETED)
    taskManager.getGraphNodeState(rightB).value should be(GraphNodeState.RUNNING)
    taskManager.runSchedulerOnce()
    taskManager.getGraphNodeState(rightB).value should be(GraphNodeState.COMPLETED)
    taskManager.getGraphNodeState(finalTask).value should be(GraphNodeState.RUNNING)
    taskManager.runSchedulerOnce()
    taskManager.getGraphNodeState(finalTask).value should be(GraphNodeState.COMPLETED)
  }


  class GetTasksExceptionTask extends Pipeline {
    override def build(): Unit = {
      throw new RuntimeException("GetTasksExceptionTask has excepted")
    }
  }

  it should "mark a task that fails when getTasks is called on it" in {
    val task: Pipeline = new GetTasksExceptionTask()
    val taskManager: TaskManager = getDefaultTaskManager
    taskManager.addTask(task)
    taskManager.runSchedulerOnce()
    taskManager.getTaskStatus(task).value should be(TaskStatus.FAILED_GET_TASKS)
  }

  // **************************************************
  // Test dependencies between workflows
  // **************************************************

  class SimplePipeline(name: String) extends Pipeline {
    withName(name)
    val firstTask: UnitTask = new NoOpInJvmTask(name=name+"-1")
    val secondTask: UnitTask = new NoOpInJvmTask(name=name+"-2")
    root ==> (firstTask :: secondTask) // do this here so we can call getTasks repeatedly
    def build(): Unit = {}
  }

  it should "complete a predecessor pipeline and all ancestors (via getTasks) before the successor pipeline" in {
    val predecessorWorkflow: SimplePipeline = new SimplePipeline("predecessor")
    val successorWorkflow: SimplePipeline = new SimplePipeline("successor")

    predecessorWorkflow.getTasks.size should be(2)
    successorWorkflow.getTasks.size should be(2)

    // no dependencies for either pipeline
    predecessorWorkflow.getTasksDependedOn.size should be(0)
    successorWorkflow.getTasksDependedOn.size should be(0)

    // predecessor should be added to the tasks for the successor
    predecessorWorkflow ==> successorWorkflow
    successorWorkflow.getTasks.size should be(2)
    predecessorWorkflow.getTasksDependedOn.size should be(0)
    successorWorkflow.getTasksDependedOn.size should be(1)

    val taskManager: TaskManager = getDefaultTaskManager
    taskManager.addTask(predecessorWorkflow)
    taskManager.addTask(successorWorkflow)

    // Run the scheduler once
    // The predecessor pipeline and its "predecessor" tasks should run, while the successor pipeline should be queued waiting
    // for the predecessor pipeline to finish.
    taskManager.runSchedulerOnce()
    taskManager.getTaskStatus(predecessorWorkflow).value should be(TaskStatus.STARTED)
    taskManager.getTaskStatus(predecessorWorkflow.firstTask).value should be(TaskStatus.STARTED)
    taskManager.getTaskStatus(predecessorWorkflow.secondTask).value should be(TaskStatus.STARTED)
    taskManager.getGraphNodeState(predecessorWorkflow).value should be(GraphNodeState.ONLY_PREDECESSORS)
    taskManager.getGraphNodeState(predecessorWorkflow.firstTask).value should be(GraphNodeState.RUNNING)
    taskManager.getGraphNodeState(predecessorWorkflow.secondTask).value should be(GraphNodeState.RUNNING)
    taskManager.getTaskStatus(successorWorkflow.firstTask) should be('empty)
    taskManager.getTaskStatus(successorWorkflow.secondTask) should be('empty)
    taskManager.getTaskStatus(successorWorkflow).value should be(TaskStatus.UNKNOWN)
    taskManager.getGraphNodeState(successorWorkflow).value should be(GraphNodeState.PREDECESSORS_AND_UNEXPANDED)

    // Run the scheduler again
    // The "predecessor" tasks should be complete, and so the successor pipeline and its "successor" tasks should be running.
    taskManager.runSchedulerOnce()
    taskManager.getTaskStatus(predecessorWorkflow).value should be(TaskStatus.SUCCEEDED)
    taskManager.getTaskStatus(predecessorWorkflow.firstTask).value should be(TaskStatus.SUCCEEDED)
    taskManager.getTaskStatus(predecessorWorkflow.secondTask).value should be(TaskStatus.SUCCEEDED)
    taskManager.getGraphNodeState(predecessorWorkflow).value should be(GraphNodeState.COMPLETED)
    taskManager.getGraphNodeState(predecessorWorkflow.firstTask).value should be(GraphNodeState.COMPLETED)
    taskManager.getGraphNodeState(predecessorWorkflow.secondTask).value should be(GraphNodeState.COMPLETED)
    taskManager.getTaskStatus(successorWorkflow).value should be(TaskStatus.STARTED)
    taskManager.getTaskStatus(successorWorkflow.firstTask).value should be(TaskStatus.STARTED)
    taskManager.getTaskStatus(successorWorkflow.secondTask).value should be(TaskStatus.STARTED)
    taskManager.getGraphNodeState(successorWorkflow).value should be(GraphNodeState.ONLY_PREDECESSORS)
    taskManager.getGraphNodeState(successorWorkflow.firstTask).value should be(GraphNodeState.RUNNING)
    taskManager.getGraphNodeState(successorWorkflow.secondTask).value should be(GraphNodeState.RUNNING)

    // Run the scheduler again
    // The successor pipeline and its "successor" tasks should be complete.
    taskManager.runSchedulerOnce()
    taskManager.getTaskStatus(successorWorkflow).value should be(TaskStatus.SUCCEEDED)
    taskManager.getTaskStatus(successorWorkflow.firstTask).value should be(TaskStatus.SUCCEEDED)
    taskManager.getTaskStatus(successorWorkflow.secondTask).value should be(TaskStatus.SUCCEEDED)
    taskManager.getGraphNodeState(successorWorkflow).value should be(GraphNodeState.COMPLETED)
    taskManager.getGraphNodeState(successorWorkflow.firstTask).value should be(GraphNodeState.COMPLETED)
    taskManager.getGraphNodeState(successorWorkflow.secondTask).value should be(GraphNodeState.COMPLETED)

    // Run the scheduler a final time
    // There should be no running readyTasks, tasksToSchedule, runningTasks, or completedTasks in this round of scheduling.
    val (readyTasks, tasksToSchedule, runningTasks, completedTasks) = taskManager.runSchedulerOnce()
    readyTasks should have size 0
    tasksToSchedule should have size 0
    runningTasks should have size 0
    completedTasks should have size 0

    // Make sure all tasks submitted to the system are completed
    for (taskId <- taskManager.getTaskIds()) {
      taskManager.getTaskStatus(taskId).value should be (TaskStatus.SUCCEEDED)
      taskManager.getGraphNodeState(taskId).value should be(GraphNodeState.COMPLETED)
    }
    taskManager.getTaskIds() should have size 6
  }

  // **************************************************
  // Test cycles in the dependency graph
  // **************************************************

  {
    it should "fail to add a task that has a cyclical dependency" in {
      val predecessor: Task = new NoOpTask withName "predecessor"
      val successor: Task = new NoOpTask withName "successor"

      // make a simple cycle
      predecessor ==> successor ==> predecessor

      val taskManager: TaskManager = getDefaultTaskManager
      an[IllegalArgumentException] should be thrownBy taskManager.addTask(predecessor)
    }

    // TODO: This test should fail due the introduction of a cycle during the pipeline.build(), but doesn't
//    it should "fail to run a task if it is found to have a cyclical dependency after getTasks is called" in {
//
//      // Set things up so that task ==> pipeline, and when pipeline.build() is called we also introduce
//      // a dependency from a pipeline_task ==> task
//      val task: Task = new NoOpTask withName "StandaloneUnitTask"
//      val naughtyPipeline = new Pipeline {
//        override def build(): Unit = {
//          val innerTask = new NoOpTask().withName("UnitTaskInPipeline")
//          root ==> innerTask
//          task ==> innerTask
//        }
//      }
//
//      naughtyPipeline ==> task
//
//      naughtyPipeline.getTasksDependedOn should have size 0
//      naughtyPipeline.getTasksDependingOnThisTask should have size 1
//      task.getTasksDependedOn should have size 1
//      task.getTasksDependingOnThisTask should have size 0
//
//      val taskManager: TaskManager = getDefaultTaskManager
//      taskManager.addTask(naughtyPipeline) should be(0)
//      taskManager.addTask(task) should be(1)
//
//      // getTasks should be called on predecessor, which now creates a cycle!
//      taskManager.runSchedulerOnce()
//      taskManager.getTaskStatus(naughtyPipeline).value should be(TaskStatus.FAILED_GET_TASKS)
//      taskManager.getTaskStatus(task).value should be(TaskStatus.UNKNOWN)
//      taskManager.getGraphNodeState(naughtyPipeline).value should be(GraphNodeState.COMPLETED)
//      taskManager.getGraphNodeState(task).value should be(GraphNodeState.PREDECESSORS_AND_UNEXPANDED)
//    }
  }

  // Test that scheduler limits are observed!
  {
    it should "not run tasks concurrently with more Cores than are defined in the system." in {
      val systemCores       = 4
      var allocatedCores    = 0
      var maxAllocatedCores = 0

      // A task that would like 1-8 cores each
      class HungryTask extends ProcessTask {
        var coresGiven = 0
        override def args = "exit" :: "0" :: Nil

        override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = {
          (8 to 1 by -1).map{ c => ResourceSet(Cores(c), Memory("1g")) }.find(rs => availableResources.subset(rs).isDefined)
        }

        override def applyResources(resources: ResourceSet): Unit = {
          coresGiven = resources.cores.toInt
          allocatedCores += coresGiven
          maxAllocatedCores = Math.max(maxAllocatedCores, allocatedCores)
        }

        override def onComplete(exitCode: Int): Boolean = {
          allocatedCores -= coresGiven
          super.onComplete(exitCode)
        }
      }

      // A pipeline with several hungry tasks that can be run in parallel
      class HungryPipeline extends Pipeline {
        override def build(): Unit = root ==> (new HungryTask :: new HungryTask :: new HungryTask)
      }

      TaskManager.run(
        new HungryPipeline,
        sleepMilliseconds = 1,
        taskManagerResources = Some(TaskManagerResources(systemCores, Resource.parseSizeToBytes("8g").toLong, 0.toLong))
      )
      maxAllocatedCores should be <= systemCores
    }
  }

  it should "handle a few thousand tasks" taggedAs LongRunningTest in {
    val numTasks = 10000
    val dependencyProbability = 0.1

    class ATask extends ProcessTask {
      override def args = "exit" :: "0" :: Nil

      override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = {
        val mem = Memory("1g")
        (8 to 1 by -1).map(c => ResourceSet(Cores(c), mem)).find(rs => availableResources.subset(rs).isDefined)
      }
    }

    // create the tasks
    val tasks = for (i <- 1 to numTasks) yield new ATask

    // make them depend on previous tasks
    val randomNumberGenerator = scala.util.Random
    for (i <- 1 until numTasks) {
      for (j <- 1 until i) {
        if (randomNumberGenerator.nextFloat < dependencyProbability) tasks(j) ==> tasks(i)
      }
    }

    // add the tasks to the task manager
    val taskManager: TaskManager = getDefaultTaskManager
    taskManager.addTasks(tasks)

    // run the tasks
    taskManager.runAllTasks(sleepMilliseconds = 1, timeout = 1)

    // make sure all tasks have been completed
    tasks.foreach { task =>
      taskManager.getTaskStatus(task).value should be(TaskStatus.SUCCEEDED)
      taskManager.getGraphNodeState(task).value should be(GraphNodeState.COMPLETED)
    }
  }

  private def getAndTestTaskExecutionInfo(taskManager: TaskManager, task: Task): TaskExecutionInfo = {
    // make sure the execution info and timestamps are set for this taske
    val info = taskManager.getTaskExecutionInfo(task)
    info shouldBe 'defined
    info.get.submissionDate shouldBe 'defined
    info.get.startDate shouldBe 'defined
    info.get.endDate shouldBe 'defined
    info.get
  }

  it should "set the submission, start, and end dates correctly for Pipelines" in {
    val taskOne = new ShellCommand("sleep", "1")
    val taskTwo = new ShellCommand("sleep", "1")
    val pipeline = new Pipeline {
      override def build(): Unit = {
        root ==> taskOne ==> taskTwo
      }
    }

    // add the tasks to the task manager
    val taskManager: TaskManager = getDefaultTaskManager
    taskManager.addTasks(pipeline)

    // run the tasks
    taskManager.runAllTasks(sleepMilliseconds = 1, timeout = 1)

    // get and check the info
    val pipelineInfo: TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, pipeline)
    val taskOneInfo : TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, taskOne)
    val taskTwoInfo : TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, taskTwo)

    // the submission and start date should match the first task, but the end date should be after since the second task
    // ends after the first
    pipelineInfo.submissionDate.get.compareTo(taskOneInfo.submissionDate.get) should be <= 0
    pipelineInfo.startDate.get.equals(taskOneInfo.startDate.get) shouldBe true
    pipelineInfo.endDate.get.after(taskOneInfo.endDate.get) shouldBe true

    // the end date should match the second task, but the second task is started after the first
    pipelineInfo.submissionDate.get.compareTo(taskTwoInfo.submissionDate.get) should be <= 0
    pipelineInfo.startDate.get.before(taskTwoInfo.startDate.get) shouldBe true
    pipelineInfo.endDate.get.equals(taskTwoInfo.endDate.get) shouldBe true
  }

  it should "set the submission, start, and end dates correctly for a Pipeline within a Pipeline" in {
    val firstTask = new ShellCommand("sleep", "1") // need to wait to make sure timestamps are updated
    val secondTask = new ShellCommand("sleep", "1") // need to wait to make sure timestamps are updated
    val innerPipeline = new Pipeline {
      override def build(): Unit = {
        root ==> secondTask
      }
    }
    val outerPipeline = new Pipeline {
      override def build(): Unit = {
        root ==> firstTask ==> innerPipeline
      }
    }
    // NB: the execution is really: root ==> firstTask ==> secondTask

    // add the tasks to the task manager
    val taskManager: TaskManager = getDefaultTaskManager
    taskManager.addTasks(outerPipeline)

    // run the tasks
    taskManager.runAllTasks(sleepMilliseconds = 1, timeout = 1)

    // get and check the info
    val firstTaskInfo : TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, firstTask)
    val secondTaskInfo : TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, secondTask)
    val innerPipelineInfo: TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, innerPipeline)
    val outerPipelineInfo: TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, outerPipeline)

    // submission dates
    // inner pipeline is submitted just before second task, while outer pipeline is submitted just before first task
    innerPipelineInfo.submissionDate.get.compareTo(secondTaskInfo.submissionDate.get) should be <= 0
    outerPipelineInfo.submissionDate.get.compareTo(firstTaskInfo.submissionDate.get) should be <= 0

    // start dates
    // - inner pipeline should have a start date of task two, while outer pipeline should have a start date of task one
    innerPipelineInfo.startDate.get.compareTo(secondTaskInfo.startDate.get) should be <= 0
    outerPipelineInfo.startDate.get.compareTo(firstTaskInfo.startDate.get) should be <= 0

    // end dates
    // - both pipelines should have end dates of task two
    innerPipelineInfo.endDate.get.compareTo(secondTaskInfo.endDate.get) should be <= 0
    outerPipelineInfo.endDate.get.compareTo(secondTaskInfo.endDate.get) should be <= 0
  }
}
