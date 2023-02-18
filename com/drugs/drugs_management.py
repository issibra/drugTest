from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, col
import pyspark.sql.functions as f

spark = SparkSession.builder.master("local").appName("Drug Links").getOrCreate()


def extract_struct(col_title, col_date, col_id):
    return f.struct(*(
            [f.when(col(col_id).isNotNull(), f.col(col_title)).otherwise(f.lit(""))]
            + [f.when(col(col_id).isNotNull(), f.col(col_date)).otherwise(f.lit(""))]))


def load():
    path_drugs = r"C:\Users\ASUS\Downloads\test_de_python_v2\test_de_python_v2\drugs.csv"
    path_trials = r"C:\Users\ASUS\Downloads\test_de_python_v2\test_de_python_v2\clinical_trials.csv"
    path_pubmed = r"C:\Users\ASUS\Downloads\test_de_python_v2\test_de_python_v2\pubmed.csv"
    path_pubmed_json = r"C:\Users\ASUS\Downloads\test_de_python_v2\test_de_python_v2\pubmed.json"

    drugs = spark.read.option("header", "true").csv(path_drugs)
    trials = spark.read.option("header", "true").csv(path_trials)
    pubmed = spark.read.option("header", "true").csv(path_pubmed).union(
        spark.read.option("multiline", "true").json(path_pubmed_json).select("id", "title", "date", "journal"))

    return {"drug": drugs, "trial": trials, "pub": pubmed}


def process_data():
    map_input = load()
    path_write = r"C:\Users\ASUS\Downloads\test_de_python_v2\test_de_python_v2\result.json"
    drugs = map_input["drug"]
    pubmed = map_input["pub"]
    trials = map_input["trial"]

    # join drug with publication to extract specific publication and journals information related to each drug
    drugsJoinPub = drugs.join(pubmed, upper(pubmed.title).contains(drugs.drug), "left")
    drugsJoinPub = drugsJoinPub.withColumn("pubs", extract_struct("title", "date", "id"))

    schema_drug = "struct<title: string, date: string>"
    drugWithPubs = drugsJoinPub \
        .select(col("atccode"), col("drug"), col("date"), col("journal"), col("pubs").cast(schema_drug))
    drugsWithPubs = drugWithPubs.withColumn("journals_p", extract_struct("journal", "date", "journal"))

    # join drug with trials to extract specific trials and journals information related to each drug
    drugsWithTrials = drugs.join(trials, upper(trials.scientific_title).contains(drugs.drug), "left")
    drugsWithTrials = drugsWithTrials.withColumn("trials",
                                                 extract_struct("scientific_title", "date", "scientific_title"))
    drugsWithTrials = drugsWithTrials.withColumn("journals_s", extract_struct("journal", "date", "journal"))

    schema_trial = "struct<title: string, date: string>"
    drugsWithTrials = drugsWithTrials.filter(col("id").isNotNull() & col("scientific_title").isNotNull()).select(
        col("atccode"), col("drug"), col("scientific_title"), col("date"), col("journal"), col("journals_s"),
        col("trials").cast(schema_trial))

    schema_j = "struct<journal: string, date: string>"
    drugsWithPubs = drugsWithPubs.select(col("atccode"), col("drug"), col("pubs"), col("journals_p").cast(schema_j)) \
        .withColumn("journals_p", f.when(col("pubs").isNull(), None).otherwise(col("journals_p"))) \
        .groupBy("atccode", "drug") \
        .agg(f.collect_set("pubs").alias("pubs"), f.collect_set("journals_p").alias("journal_p"))

    drugsWithTrials = drugsWithTrials.select(col("atccode"), col("drug"), col("trials"),
                                             col("journals_s").cast(schema_j)) \
        .withColumn("journals_s", f.when(col("trials").isNull(), None).otherwise(col("journals_s"))) \
        .groupBy("atccode", "drug") \
        .agg(f.collect_set("trials").alias("trials"), f.collect_set("journals_s").alias("journals_s"))

    ## coalesce is necessary to elimnate null values from array union of trial journal and pub journal
    drugsWithJournals = drugsWithPubs.join(drugsWithTrials, ["drug", "atccode"], "left") \
        .withColumn("journal",
                    f.array_union(f.coalesce(col("journals_s"), f.array()), f.coalesce(col("journal_p"), f.array()))) \
        .drop("journal_p", "journals_s")

    drugsWithJournals.coalesce(1).write.format("json").save(path_write)
