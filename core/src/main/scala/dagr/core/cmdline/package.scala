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
package dagr.core

import dagr.core.tasksystem.Pipeline

package object cmdline {
  /**
    * Used to annotate which fields of a CommandLineTask are options given at the command line.
    * If a command line call looks like "cmd option=foo x=y bar baz" the CommandLineTask
    * would have annotations on fields to handle the values of option and x. All options
    * must be in the form name=value on the command line. The java type of the option
    * will be inferred from the type of the field or from the generic type of the collection
    * if this option is allowed more than once. The type must be an enum or
    * have a constructor with a single String parameter.
    */
  type Arg = ArgAnnotation

  /**
    * Annotation to be placed on Piplines that are to be exposed as command line programs.
    */
  type CLP = CLPAnnotation

  /**
    * Annotation that can be used to disambiguiate which constructor should be used for a CLP
    * pipeline when all conventions fail.
    */
  type CLPConstructor = CLPConstructorAnnotation

  /** Type to represent the kind of Class that is runnable from the command line, since it's used in many places. */
  type PipelineClass = Class[_ <: Pipeline]
}
