object userDefinedMetrics
{
  def evaluateModel( labelAndPreds: RDD[(Double, Double)]) : String =  
    {         
       val testingSamplesNumber = labelAndPreds.count
       val wrongPrdictionsNumber = 
         labelAndPreds
         .filter{
           case(label,pred) => (label != pred)
                }
         .count.toDouble
     
       val testErr = wrongPrdictionsNumber / testingSamplesNumber
  
       type Sum = (Int, Int, Int, Int, Int, Int, Int, Int, Int)
       val seqOp = {(s1: Sum, s2: (Int, Int, Int, Int, Int, Int, Int, Int, Int)) => (s1._1+s2._1,s1._2+s2._2,s1._3+s2._3,s1._4+s2._4,s1._5+s2._5,s1._6+s2._6,s1._7+s2._7,s1._8+s2._8,s1._9+s2._9)}    
       val combOp = {(s1: Sum, s2: Sum) => (s1._1+s2._1,s1._2+s2._2,s1._3+s2._3,s1._4+s2._4,s1._5+s2._5,s1._6+s2._6,s1._7+s2._7,s1._8+s2._8,s1._9+s2._9)}
       val confusionMatrixClassCounters =
         labelAndPreds
         .map{case(label,pred) => (
             if((label == LABEL_X) && (pred == LABEL_X)) 1 else 0,
             if((label == LABEL_X) && (pred == LABEL_Y)) 1 else 0,
             if((label == LABEL_X) && (pred == LABEL_Z)) 1 else 0,
             if((label == LABEL_Y) && (pred == LABEL_X)) 1 else 0,
             if((label == LABEL_Y) && (pred == LABEL_Y)) 1 else 0,
             if((label == LABEL_Y) && (pred == LABEL_Z)) 1 else 0,
             if((label == LABEL_Z) && (pred == LABEL_X)) 1 else 0,
             if((label == LABEL_Z) && (pred == LABEL_Y)) 1 else 0,
             if((label == LABEL_Z) && (pred == LABEL_Z)) 1 else 0
                   )}
         .aggregate((0,0,0,0,0,0,0,0,0))(seqOp, combOp)
     
      val TPX = confusionMatrixClassCounters._1
      val FPX = confusionMatrixClassCounters._4 + confusionMatrixClassCounters._7 
      val TNX = confusionMatrixClassCounters._5 +  confusionMatrixClassCounters._6 + confusionMatrixClassCounters._8 + confusionMatrixClassCounters._9
      val FNX = confusionMatrixClassCounters._2 + confusionMatrixClassCounters._3
      val PX = TPX + FPX
      val LX = TPX + FNX 
  
      val TPY = confusionMatrixClassCounters._5
      val FPY = confusionMatrixClassCounters._2 + confusionMatrixClassCounters._8 
      val TNY = confusionMatrixClassCounters._1 +  confusionMatrixClassCounters._3 + confusionMatrixClassCounters._7 + confusionMatrixClassCounters._9
      val FNY = confusionMatrixClassCounters._4 + confusionMatrixClassCounters._6
      val PY = TPY + FPY 
      val LY = TPY + FNY
  
      val TPZ = confusionMatrixClassCounters._9
      val FPZ = confusionMatrixClassCounters._3 + confusionMatrixClassCounters._6 
      val TNZ = confusionMatrixClassCounters._1 +  confusionMatrixClassCounters._2 + confusionMatrixClassCounters._4 + confusionMatrixClassCounters._5
      val FNZ = confusionMatrixClassCounters._7 + confusionMatrixClassCounters._8
      val PZ = TPZ + FPZ
      val LZ = TPZ + FNZ
          
      val TPA: Double = (TPX+TPY+TPZ)/3.0
      val FPA: Double = (FPX+FPY+FPZ)/3.0
      val TNA: Double = (TNX+TNY+TNZ)/3.0
      val FNA: Double = (FNX+FNY+FNZ)/3.0
      val PA = TPA + FPA
      val LA = TPA + FNA
  
      val x_Sensitivity: Double = TPX.toDouble/(LX)
      val x_Specificity: Double = TNX.toDouble/(TNX+FPX)
      val x_Precision: Double = TPX.toDouble/(PX)
      val x_RealCoverage: Double = LX.toDouble/testingSamplesNumber
      val x_PredCoverage: Double = PX.toDouble/testingSamplesNumber
  
      val y_Sensitivity: Double = TPY.toDouble/(TPY+FNY)
      val y_Specificity: Double = TNY.toDouble/(TNY+FPY)
      val y_Precision: Double = TPY.toDouble/(PY)
      val y_RealCoverage: Double = LY.toDouble/testingSamplesNumber
      val y_PredCoverage: Double = PY.toDouble/testingSamplesNumber
  
      val z_Sensitivity: Double = TPZ.toDouble/(TPZ+FNZ)
      val z_Specificity: Double = TNZ.toDouble/(TNZ+FPZ)
      val z_Precision: Double = TPZ.toDouble/(PZ)
      val z_RealCoverage: Double = LZ.toDouble/testingSamplesNumber
      val z_PredCoverage: Double = PZ.toDouble/testingSamplesNumber
  
      val averageSensitivity: Double = TPA.toDouble/(TPA+FNA)
      val averageSpecificity: Double = TNA.toDouble/(TNA+FPA)
      val averagePrecision: Double = TPA.toDouble/(PA)
      val averageRealCoverage: Double = LA.toDouble/testingSamplesNumber
      val averagePredCoverage: Double = PA.toDouble/testingSamplesNumber
   
       val modelEvaluationString = 
       "Testing samples number = " + testingSamplesNumber + "\n" +
       "Right predictions: " + (testingSamplesNumber-wrongPrdictionsNumber) +", " + "Wrong predictions: " + wrongPrdictionsNumber + "\n" +
       "Test Error = " + testErr + "\n" +
       "Average model stats : " + "\n" +
       "Sensitivity = " + averageSensitivity + "\n" +
       "Specificity = " + averageSpecificity + "\n" +
       "Precision = " + averagePrecision + "\n" +
       "Real Coverage = " + averageRealCoverage + "\n" +
       "Pred Coverage = " + averagePredCoverage + "\n" +
       "X class stats : " + "\n" +
       "Sensitivity = " + x_Sensitivity + "\n" +
       "Specificity = " + x_Specificity + "\n" +
       "Precision = " + x_Precision + "\n" +
       "Real Coverage = " + x_RealCoverage + "\n" +
       "Pred Coverage = " + x_PredCoverage + "\n" +
       "Y class stats : " + "\n" +
       "Sensitivity = " + y_Sensitivity + "\n" +
       "Specificity = " + y_Specificity + "\n" +
       "Precision = " + y_Precision + "\n" +
       "Real Coverage = " + y_RealCoverage + "\n" +
       "Pred Coverage = " + y_PredCoverage + "\n" +
       "Z class stats : " + "\n" +
       "Sensitivity = " + z_Sensitivity + "\n" +
       "Specificity = " + z_Specificity + "\n" +
       "Precision = " + z_Precision + "\n" +
       "Real Coverage = " + z_RealCoverage + "\n" +
       "Pred Coverage = " + z_PredCoverage + "\n" 
  
       modelEvaluationString
    }
}
