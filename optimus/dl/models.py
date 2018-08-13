from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.image import ImageSchema
from sparkdl import DeepImageFeaturizer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from sparkdl import DeepImagePredictor

from optimus import Optimus

op = Optimus()


class DL:
    @staticmethod
    def image_classifier_lr(df, input_col="image", output_col="features", model_name="InceptionV3"):
        featurizer = DeepImageFeaturizer(inputCol=input_col, outputCol=output_col, modelName=model_name)
        lr = LogisticRegression(maxIter=10, regParam=0.05, elasticNetParam=0.3, labelCol="label")
        p = Pipeline(stages=[featurizer, lr])
        p_model = p.fit(df)
        return p_model, p_model.transform(df)

    @staticmethod
    def evaluate_image_lr(df, model):
        tested_df = model.transform(df)
        evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
        return evaluator.evaluate(tested_df.select("prediction", "label"))

    @staticmethod
    def image_predictor(path, input_col="image", output_col="predicted_labels", model_name="InceptionV3",
                        decode_predictions=True, topK=10):
        image_df = ImageSchema.readImages(path)
        predictor = DeepImagePredictor(inputCol=input_col, outputCol=output_col, modelName=model_name,
                                       decodePredictions=decode_predictions, topK=topK)
        return predictor.transform(image_df)
