#pip install flask
from flask import Flask, render_template, request
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType


app = Flask(__name__)

# Initialiser une session Spark
spark = SparkSession.builder.appName("RoadSafetyApp").getOrCreate()

# Charger le modèle PySpark
model_path = "hdfs://127.0.0.1:9000/Model"
model = PipelineModel.load(model_path)

# Route pour la page d'accueil
@app.route('/')
def home():
    return render_template('index.html')

# Route pour effectuer des vérifications
@app.route('/VerifyNews', methods=['POST'])
def VerifyNews():
    # Récupérer les données de la requête POST (News)
    data = request.form.to_dict()

    # Définir le schéma avec le type de données approprié
    schema = StructType([
        StructField("News", StringType(), True)
    ])

    # Créer une liste de tuples contenant les données de la requête
    input_data = [(data['News'],)]

    # Créer un DataFrame Spark à partir de la liste de tuples en utilisant le schéma défini
    input_df = spark.createDataFrame(input_data, schema=schema)

    # Renommer la colonne "News" en "text" pour correspondre au modèle
    input_df = input_df.withColumnRenamed("News", "text")

    # Utiliser le modèle pour effectuer les vérifications
    verifications = model.transform(input_df)

    # Récupérer la vérification
    verification = verifications.select("prediction").first()[0]

    # Formater la vérification pour l'afficher dans la page HTML
    verification_text = "Fake News" if verification == 1 else "Real News"

    # Retourner la vérification à la page HTML
    return render_template('index.html', verification_text=verification_text)

if __name__ == '__main__':
    app.run(debug=True)
