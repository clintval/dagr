package dagr.tasks.picard

import dagr.core.execsystem.{Cores, Memory}
import dagr.core.tasksystem.Pipe
import dagr.tasks.DagrDef.{PathToBam, PathToIntervals}
import dagr.tasks.DataTypes.SamOrBam
import dagr.tasks.picard.PicardTask

import scala.collection.mutable.ListBuffer

class ViewSam(in: PathToBam, intervals: Option[PathToIntervals]) extends PicardTask with Pipe[SamOrBam, SamOrBam] {

  requires(Cores(1), Memory("512M"))

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("INPUT=" + in)
    buffer.append("ALIGNMENT_STATUS=All")
    buffer.append("PF_STATUS=All")

    intervals.foreach(_intervals => buffer.append("INTERVAL_LIST=" + _intervals))
  }
}
