object ModelingWrapper
{
    def runScoreAB( featureMatrixReader : FeatureMatrixReader,
                          conf: Map[String, String]) : DataFrame=
    {
    
      val data = featureMatrixReader.readFeatureMatrix
      val scoreList =
      conf.seq.map{case(modelPath,userListPath) =>
        val model = readModel(modelPath)
        val users = sqlc.sparkContext.textFile(userListPath, 100).toDF()
        val validRecords= 
          data.as("data")
          .join(users.as("us"),
                $"data.user_id" === $"us.user_id",
                "inner")
        val scores = scoreByModel(validRecords, model)
        scores
      }
    
      val firstScoreFile = scoreList.head
      val restOfScores = scoreList.takeRight(scoreList.size-1) 
      val scores = 
        restOfScores.foldLeft(firstScoreFile){
        (scoreFile1, scoreFile2) => scoreFile1.unionAll(scoreFile2)}
    
      scores
    }
  
  
	def runScorePipeline( featureMatrixReader : FeatureMatrixReader,
	                        modelInputLocation: String) : DataFrame=
	  {
    
	    val data = featureMatrixReader.readFeatureMatrix
	    val model = readModel(modelInputLocation)
	    val scores = scoreByModel(data, model)
    
	    scores
	  }
  
	  def runTrainPipeline( featureMatrixReader : FeatureMatrixReader, labelColumm: String, outputDir: String) =
	  {
	    val data = featureMatrixReader.readFeatureMatrix
	    val (trainingData, testingData) =
	        userBasedDataSplit(data)
	    val pipeline = buildModel(data, labelColumm)
	    val model = trainModel(trainingData, pipeline)
	    val accuracy = evaluateModel(testingData, model, labelColumm)
	    saveModel(model, outputDir)
	  }
  
	   def saveModel(model: CrossValidatorModel, outputDir: String) =
	   {
	     sqlc.sparkContext.parallelize(Seq(model), 1).saveAsObjectFile(outputDir)
	   }
   
	   def readModel(modelInputLocation: String) : CrossValidatorModel =
	   {
	     val model = sqlc.sparkContext.objectFile[CrossValidatorModel](modelInputLocation).first()
	     model
	   }
	   
	   def buildModel(data: DataFrame, labelColumm: String) : Pipeline =
	   {
	     val indexedLabelColumn = getIndexedLabelColumnName(labelColumm) 
      
	     // Group features to feature vector
	     val featureAssembler = new VectorAssembler()
	       .setInputCols(FEATURE_NAMES)
	       .setOutputCol(RAW_FEATURE_VECTOR)
    
	     // Automatically identify categorical features, and index them.
	     // Set maxCategories so features with > 
	       //MAX_CATEGORIES_FOR_CATEGORICAL_FEATURES distinct values are treated as continuous.
	     val featureIndexer = new VectorIndexer()
	       .setInputCol(RAW_FEATURE_VECTOR)
	       .setOutputCol(INDEXED_FEATURE_VECTOR)
	       .setMaxCategories(MAX_CATEGORIES_FOR_CATEGORICAL_FEATURES)
  
	     val labelIndexer = new StringIndexer()
	       .setInputCol(labelColumm)
	       .setOutputCol(indexedLabelColumn)
	       .fit(data)
      
	     // Train a RandomForest model.
	     val randomForest = new RandomForestClassifier()
	       .setLabelCol(indexedLabelColumn)
	       .setFeaturesCol(INDEXED_FEATURE_VECTOR)
	       .setNumTrees(NUMBER_OF_TREES)
	       .setMaxDepth(MAX_DEPTH)
	       .setSeed(RANDOM_SEED)
	       .setSubsamplingRate(SUBSAMPLING_RATE)
      
	     val labelConverter = new IndexToString()
	       .setInputCol(indexedLabelColumn)
	       .setOutputCol(PREDICTED_LABEL)
	       .setLabels(labelIndexer.labels)
  
	     // Chain indexers and forest in a Pipeline
	     val pipeline = new Pipeline()
	       .setStages(Array(featureAssembler, featureIndexer, labelIndexer, randomForest, labelConverter))
  
	     pipeline 
	   }
  
	    def trainModel(trainingData: DataFrame, pipeline: Pipeline): CrossValidatorModel =
	   {
	     val paramGrid = 
	      new ParamGridBuilder()
	     .addGrid(pipeline.getStages(5).asInstanceOf[RandomForestClassifier]
	               .numTrees,Array(200,500,1000))
	     .addGrid(pipeline.getStages(5).asInstanceOf[RandomForestClassifier]
	               .maxDepth,Array(20,50,100))
	     .build()

	     val cv = new CrossValidator()
	     .setEstimator(pipeline)
	     .setEvaluator(new MulticlassClassificationEvaluator)
	     .setEstimatorParamMaps(paramGrid)
	     .setNumFolds(10)

	     val cvModel = cv.fit(trainingData)
	     cvModel
	   }
  
	    def evaluateModel(testingData: DataFrame,  model: CrossValidatorModel, labelColumm: String): Double =
	    {
	      val predictions = model.transform(testingData)

	      val indexedLabelColumn = getIndexedLabelColumnName(labelColumm) 
     
	      val precisionEvaluator = new MulticlassClassificationEvaluator()
	         .setLabelCol(indexedLabelColumn)
	         .setPredictionCol(PREDICTED_INDEXED_LABEL)
	         .setMetricName(CLASSIFICATION_METRIC_PRECISION)
        
	      val accuracy = precisionEvaluator.evaluate(predictions)
     
	      println("Test Error (1-precision) = " + (1.0 - accuracy))
     
	       val recallEvaluator = new MulticlassClassificationEvaluator()
	         .setLabelCol(indexedLabelColumn)
	         .setPredictionCol(PREDICTED_INDEXED_LABEL)
	         .setMetricName(CLASSIFICATION_METRIC_RECALL)
        
	      val recall = recallEvaluator.evaluate(predictions)
     
	      println("Recall (sensitivity) = " + recall)
     
	      accuracy
	    }

   
	    def scoreByModel(data: DataFrame, model: CrossValidatorModel): DataFrame =
	    {
	      val predictions = model.transform(data)
             
	      val scores =
	        predictions
	        .selectExpr(
	            "user_id",
	            "cur_event_date",
	            "cur_lon",
	            "cur_lat",
	            PREDICTED_INDEXED_LABEL,
	            PREDICTED_PROBABILITY //probability vector per class
	            )
	         .map { case(prow) => (
	                   prow.getLong(0),
	                   prow.getTimestamp(1),
	                   prow.getDouble(2),
	                   prow.getDouble(3),
	                   prow.getDouble(4),
	                   // get probability of predicted class only
	                   ((prow.getAs[Vector](5)).apply(prow.getDouble(4).toInt)).toDouble
	                 )}
	         .toDF(
	             "user_id",
	             "cur_event_date",
	             "cur_lon",
	             "cur_lat",
	             PREDICTED_INDEXED_LABEL,
	             PREDICTED_PROBABILITY
	             )
            
	      scores
	    }
}

