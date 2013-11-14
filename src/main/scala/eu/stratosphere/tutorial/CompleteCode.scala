package eu.stratosphere.tutorial

import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription

import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._

import scala.collection.mutable.HashMap
import scala.io.Source

class CompleteCode extends PlanAssembler with PlanAssemblerDescription with Serializable {
  
  override def getDescription() = {
    "Usage: [inputPath] [outputPath] ([numSubtasks])"
  }
  
  override def getPlan(args: String*) = {
    val inputPath = args(0)
    val outputPath = args(1)
    val numDocuments = if (args.size >= 3) args(2).toInt else Util.NUM_DOCUMENTS;
    val numSubTasks = if (args.size >= 4) args(3).toInt else 1
    
    val source = TextFile(inputPath)
    
    // Solution for Task 1
    val termOccurences = source flatMap { line =>
      line.indexOf(',') match {
        case -1 => None
        case pos =>
          val (docId, doc) = line.splitAt(pos)
          doc.toLowerCase()
            .split("""\W+""")
            .filter { !Util.STOP_WORDS.contains(_) }
            .distinct
      }
    }
    
    val documentFrequencies = termOccurences
      .groupBy { w => w }
      .count()
    
    // Solution for Task 2
    val termFrequencies = source flatMap { line =>
      line.indexOf(',') match {
        case -1 => None
        case pos =>
          val (docId, doc) = line.splitAt(pos)
          doc.toLowerCase()
            .split("""\W+""")
            .filter { !Util.STOP_WORDS.contains(_) }
            .foldLeft(new HashMap[String, Int]) { (map, word) =>
              map get(word) match {
                case Some(x) => map += (word -> (x+1))   //if in map already, increment count
                case None => map += (word -> 1)          //otherwise, set to 1
              }
            }
            .map { case (word, count) => (docId, word, count) }
      }
    }
    
    // Solution for Task 3
    val  tfIdf = documentFrequencies
      .join(termFrequencies)
      .where { _._1 }
      .isEqualTo { _._2 }
      .map { (left, right) =>
        val (word, docFreq) = left
        val (docId, _, termFreq) = right
        (docId, word, termFreq * Math.log(numDocuments / docFreq))
    }
      
    // Solution for Task 4
    val tfIdfPerDocument = tfIdf
      .groupBy { _._1 }
      .reduceGroup { values =>
        val buffered = values.buffered
        val (docId, _, _) = buffered.head
        val weightList = buffered map { t => (t._2, t._3) }
        
        // sort the list after the weights and take only the 15 terms with largest weights
        // this step is optional, it truncates the long vectors for large pages
        val weights = weightList.toArray.sortWith((x, y) => x._2 > y._2).take(15)
        
        WeightVector(docId, weights)
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

object RunCompleteCode {
  def main(args: Array[String]) {
    
    // Write test input to temporary directory
    // swap these four lines for the commented two lines succeeding them to try the algorithm
    // on sample of cleansed wikipedia data.
    val inputPath = Util.createTempDir("input")
    Util.createTempFile("input/1.txt", "1,Big Hello to Stratosphere! :-)")
    Util.createTempFile("input/2.txt", "2,Hello to Big Big Data.")
    val numDocs = Util.NUM_DOCUMENTS;
    
//    val inputPath = "file:///home/stratosphere/demodata/text/wikipedia.txt"
//    val numDocs = 10000;
    
    // Output
    // Replace this with your own path, e.g. "file:///path/to/results/"
    val outputPath = "/home/stratosphere/tf-idf-out"

    // Results should be: same Tf-Idf values as in task 3 as a WeightVector per Document

    println("Reading input from " + inputPath)
    println("Writing output to " + "file://" + outputPath)

    val plan = new CompleteCode().getPlan(inputPath, "file://" + outputPath, numDocs.toString)
    Util.executePlan(plan)

    println("Result in " + outputPath + ":")
    for(line <- Source.fromFile(outputPath).getLines())
      println(line)

    Util.deleteAllTempFiles()
    System.exit(0)
  }
}
