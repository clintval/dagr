/*
 * The MIT License
 *
 * Copyright (c) 2016 Fulcrum Genomics LLC
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

import dagr.core.tasksystem.ShellCommand
import dagr.core.util.UnitSpec

class GraphNodeTest extends UnitSpec {

  "GraphNode.addPredecessors" should "return true if a predecessor is added twice" in {
    val node = new GraphNode(taskId=BigInt(0), task=new ShellCommand("make it so"))

    // add itself
    node.addPredecessors(node) shouldBe false
    node.hasPredecessor shouldBe true
    // add itself
    node.addPredecessors(node) shouldBe true
    node.hasPredecessor shouldBe true
    // remove itself
    node.removePredecessor(node) shouldBe true
    node.hasPredecessor shouldBe false
    // add itself twice
    node.addPredecessors(node, node) shouldBe true
    node.hasPredecessor shouldBe true
  }

  "GraphNode.getOriginalPredecessors" should "return only original predecssors" in {
    val alice = new GraphNode(taskId=BigInt(0), task=new ShellCommand("make it so"))
    val bob = new GraphNode(taskId=BigInt(0), task=new ShellCommand("make it so"), predecessorNodes = Seq(alice))

    alice.originalPredecessors shouldBe List.empty
    bob.originalPredecessors shouldBe List(alice)
    bob.addPredecessors(bob)
    bob.originalPredecessors shouldBe List(alice, bob)
    bob.removePredecessor(alice)
    bob.originalPredecessors shouldBe List(alice, bob) // alice was removed, but we should still return alice
  }
}
