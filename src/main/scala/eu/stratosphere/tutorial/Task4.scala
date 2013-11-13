/*
package eu.stratosphere.tutorial

import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._
import scala.collection.mutable.HashMap
import scala.io.Source

class Task4 extends PlanAssembler with PlanAssemblerDescription with Serializable {
  override def getDescription() = {
    "Usage: [inputPath] [outputPath] ([numSubtasks])"
  }

  override def getPlan(args: String*) = {
    val inputPath = args(0)
    val outputPath = args(1)
    val numSubTasks = if (args.size >= 3) args(2).toInt else 1

    val source = TextFile(inputPath)

    // Solution for Task 1
    val termOccurences = source flatMap { line =>
      val Array(docId, doc) = line.split(",")
      doc.toLowerCase()
        .split("""\W+""")
        .filter { !Util.STOP_WORDS.contains(_) }
        .toSet[String]
        .map { w => (w, 1) }
    }

    val documentFrequencies = termOccurences
      .groupBy { case (w, _) => w }
      .reduce { (w1, w2) => (w1._1, w1._2 + w2._2) }

    // Solution for Task 2
    val termFrequencies = source flatMap { line =>
      val Array(docId, doc) = line.split(",")
      doc.toLowerCase()
        .split("""\W+""")
        .filter { !Util.STOP_WORDS.contains(_) }
        .foldLeft(new HashMap[String, Int]) { (map, word) =>
          map get (word) match {
            case Some(x) => map += (word -> (x + 1)) //if in map already, increment count
            case None => map += (word -> 1) //otherwise, set to 1
          }
        }
        .map { case (word, count) => (docId, word, count) }
    }

    // Solution for Task 3
    val tfIdf = documentFrequencies
      .join(termFrequencies)
      .where { case (w, _) => w }
      .isEqualTo { case (_, w, _) => w }
      .map { (left, right) =>
        val (word, docFreq) = left
        val (docId, _, termFreq) = right
        (docId, word, termFreq * Math.log(Util.NUM_DOCUMENTS / docFreq))
      }

    // This is Task4
    val tfIdfPerDocument = tfIdf
      .groupBy {}
      .reduceGroup { values =>
      }

    val sink = tfIdfPerDocument.write(outputPath, DelimitedOutputFormat(formatOutput _))

    new ScalaPlan(Seq(sink))
  }

  def formatOutput(w: WeightVector) = {
    val terms = w.terms map { case (word, tfIdf) => word + ", " + tfIdf }
    w.docId + ": " + terms.mkString("; ")
  }

  case class WeightVector(docId: String, terms: Iterator[(String, Double)])
}

object RunTask4 {
  def main(args: Array[String]) {
    // Write test input to temporary directory
    val inputPath = Util.createTempDir("input")

    Util.createTempFile("input/1.txt", "1,Big Hello to Stratosphere! :-)")
    Util.createTempFile("input/2.txt", "2,Hello to Big Big Data.")

    // Output
    // Replace this with your own path, e.g. "file:///path/to/results/"
    val outputPath = "/home/stratosphere/tf-idf-out"

    // Results should be: same Tf-Idf values as in task 3 as a WeightVector per Document

    println("Reading input from " + inputPath)
    println("Writing output to " + "file://" + outputPath)

    val plan = new Task4().getPlan(inputPath, "file://" + outputPath)
    Util.executePlan(plan)

    println("Result in " + outputPath + ":")
    for (line <- Source.fromFile(outputPath).getLines())
      println(line)

    Util.deleteAllTempFiles()
    System.exit(0)
  }
}
*/
