from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, Word2Vec, StringIndexer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Création d'une Session Spark
spark = SparkSession.builder \
    .appName("Classification du Real/Fake News") \
    .getOrCreate()

# Charger les Données à partir d'Hadoop
data = spark.read.option("delimiter", ";").csv("hdfs://127.0.0.1:9000/Input_Data/data_News.csv", inferSchema=True, header=True)

# Renommer les Colonnes
data = data.withColumnRenamed("News", "text").withColumnRenamed("Value", "label")

# Rechercher les étiquettes non valides
invalid_labels = data.filter(~data["label"].isin([0, 1]))
if invalid_labels.count() > 0:
    print("Invalid labels found in the dataset:")
    invalid_labels.show()
    print("Please ensure all labels are 0 or 1.")
    # Détéction des étiquettes non valides, par exemple en les supprimant de l'ensemble de données ou en les corrigeant
    # Exit du Program
    spark.stop()
    exit()

# Création du Tokenizer
tokenizer = Tokenizer(inputCol="text", outputCol="words")

# Création du Modèle Word2Vec
word2vec = Word2Vec(vectorSize=100, minCount=1, inputCol="words", outputCol="word_vectors")

# Convertir la Colonne « label » en type Numérique
indexer = StringIndexer(inputCol="label", outputCol="label_index")

# Création du Modèle MLP
layers = [100, 50, 2]
mlp = MultilayerPerceptronClassifier(layers=layers, blockSize=128, seed=1234, featuresCol="word_vectors", labelCol="label_index")

# Création du Pipeline
pipeline = Pipeline(stages=[tokenizer, word2vec, indexer, mlp])

# Séparation des Données entre : les training data and test data
train_data, test_data = data.randomSplit([0.8, 0.2], seed=1234)

# Entrainer le modèle créé
model = pipeline.fit(train_data)

# Évaluer le modèle créé avec les tests data
predictions = model.transform(test_data)

# Évaluer sa précision
evaluator = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

print("Accuracy:", accuracy)

# Sauvegarde du modèle dans Hadoop
model.save("hdfs://127.0.0.1:9000/Model")

# Arrêt de la session Spark
spark.stop()
