import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly._

object BookCorpusVisualization {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("BookCorpusVisualization")
      .config("spark.master", "local[*]")
      .getOrCreate()

    import spark.implicits._

    // Charger les données nettoyées
    val cleanedDF = spark.read.parquet("C:\\Users\\mlawal\\Documents\\cleaned_bookcorpus")

    // Calcul des statistiques descriptives : nombre de mots par ligne
    val wordCountDF = cleanedDF.withColumn("wordCount", size(split($"value", " ")))

    // Afficher quelques lignes pour vérifier les données préparées
    wordCountDF.show(10)

    // Afficher les statistiques de base
    wordCountDF.describe("wordCount").show()

    // Conversion en liste pour Plotly
    val wordCounts = wordCountDF.groupBy("wordCount").count().collect().map(row => (row.getInt(0), row.getLong(1))).toList

    // Limiter le nombre de points affichés pour une meilleure lisibilité
    val maxWordCount = 1000  // Vous pouvez ajuster cette valeur selon vos besoins
    val filteredWordCounts = wordCounts.filter(_._1 <= maxWordCount)

    // Préparation des données pour Plotly
    val data = Seq(
      Bar(
        x = filteredWordCounts.map(_._1),
        y = filteredWordCounts.map(_._2),
        marker = Marker(color = Color.RGB(31, 119, 180)) // Couleur bleue
      )
    )

    // Configuration de la mise en page pour Plotly
    val layout = Layout(
      title = "Distribution du Nombre de Mots",
      xaxis = Axis(title = "Nombre de Mots"),
      yaxis = Axis(title = "Fréquence", `type` = AxisType.Log),
      bargap = 0.2,  // Espacement entre les barres pour une meilleure lisibilité
      barmode = BarMode.Stack // Mode empilé pour une meilleure présentation
    )

    // Générer et sauvegarder le graphique
    plot("distribution_nombre_de_mots.html", data, layout)

    spark.stop()
  }
}
