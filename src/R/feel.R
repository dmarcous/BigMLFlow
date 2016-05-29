# actually already exsits in R
#iris <- read.csv("iris.csv")
data <- data.frame(rnd = runif(nrow(iris)), iris)
str(data)
summary(data)

# Divide to training / testing
library(caret)
set.seed(666)
inTraining <- createDataPartition(data$Species, p = .75, list = FALSE)
training <- as.data.frame(data)[ inTraining,]
testing  <- as.data.frame(data)[-inTraining,]

# Feature Selection
#------------------

# T test on logistic regression
fit_log <- glm(as.numeric(Species) ~ ., data=training, family="gaussian")
summary(fit_log)

features <- as.matrix(training[,1:(ncol(training)-1)])
labels <- as.numeric(training$Species)
  
# Lasso (L1)
library(lars)
cv_lars <- lars::cv.lars(features,labels,plot.it = TRUE, mode = "step")
idx <- which.max(cv_lars$cv - cv_lars$cv.error <= min(cv_lars$cv))
coef(lars::lars(features,labels))[idx,]

# Elastic net
library(glmnet)
cv_glmnet <- cv.glmnet(features,labels)
coef(cv_glmnet, s = "lambda.1se")
fit_glmnet <- glmnet(features,labels,lambda=cv_glmnet$lambda.1se,alpha=0.9)
predict(fit_glmnet, type="coefficients")
fit_glmnet <- glmnet(features,labels,lambda=cv_glmnet$lambda.1se,alpha=0.95)
predict(fit_glmnet, type="coefficients")

# Modeling
#---------
testing_data<-testing[,1:(ncol(testing)-1)]

library(randomForest)
fit_forest <- randomForest(Species ~ ., data=training)
response_forest <- predict(fit_forest,newdata=testing_data)
  
library(xgboost)
fit_xgboost <- xgboost(data=features,label=training$Species,nrounds=35)
response_xgboost <- predict(fit_xgboost,newdata=as.matrix(testing_data))
response_xgboost <- round(response_xgboost)

# Evaluating
#-----------
RMSE = function(m, o){
  sqrt(mean((m - o)^2))
}

RMSE(as.numeric(response_forest),as.numeric(testing$Species))
RMSE(response_xgboost,as.numeric(testing$Species))

# Parallel modeling 
#------------------
library(doMC)
registerDoMC(cores=30)

fit_big_forest <- 
  foreach(ntree=rep(10000, 6),
          .combine=combine, .multicombine=TRUE,
          .packages='randomForest') %dopar% {
              randomForest(Species ~ ., data=training)
          }

response_big_forest <- predict(fit_big_forest,newdata=testing_data)
RMSE(as.numeric(response_big_forest),as.numeric(testing$Species))
