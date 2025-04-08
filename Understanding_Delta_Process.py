
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import sum as _sum, lit, col, max as _max
from delta.tables import DeltaTable
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ETLPipeline")

@dataclass
class PartitionInfo:
    aa: int
    bb: int
    cc: str
    dd: str

class DeltaLakeETL:
    def __init__(self, spark_session: SparkSession, staging_path: str, final_path: str, checkpoint_path: str) -> None:
        self.spark: SparkSession = spark_session
        self.staging_path: str = staging_path
        self.final_path: str = final_path
        self.checkpoint_path: str = checkpoint_path
        self.current_staging_version: Optional[int] = None

    def _write_checkpoint(self, df: DataFrame) -> None:
        if DeltaTable.isDeltaTable(self.spark, self.checkpoint_path):
            df.write.format("delta").mode("append").save(self.checkpoint_path)
        else:
            logger.info("Checkpoint table does not exist. Creating with overwrite.")
            df.write.format("delta").mode("overwrite").save(self.checkpoint_path)

    def _mark_version_merged(self, version: int, partitions: List[PartitionInfo]) -> None:
        checkpoint_data = [{
            "processed_version": version,
            "status": "merged",
            "update_time": datetime.now().isoformat(),
            "merged_partitions": json.dumps([asdict(p) for p in partitions])
        }]
        df = self.spark.createDataFrame(checkpoint_data)
        self._write_checkpoint(df)

    def _finalize_checkpoint(self, version: int) -> None:
        success_data = [{
            "processed_version": version,
            "status": "success",
            "update_time": datetime.now().isoformat(),
            "merged_partitions": None
        }]
        df = self.spark.createDataFrame(success_data)
        self._write_checkpoint(df)

    def _get_checkpoint_status(self) -> Tuple[Optional[int], Optional[str], Optional[List[PartitionInfo]]]:
        if DeltaTable.isDeltaTable(self.spark, self.checkpoint_path):
            df = self.spark.read.format("delta").load(self.checkpoint_path)
            latest = df.orderBy(F.col("processed_version").desc()).limit(1).collect()
            if latest:
                row = latest[0]
                merged_parts = json.loads(row["merged_partitions"]) if row["merged_partitions"] else None
                partitions = [PartitionInfo(**p) for p in merged_parts] if merged_parts else None
                return row["processed_version"], row["status"], partitions
        return None, None, None

    def _initialize_checkpoint(self) -> Optional[int]:
        try:
            if DeltaTable.isDeltaTable(self.spark, self.checkpoint_path):
                checkpoint_df = self.spark.read.format("delta").load(self.checkpoint_path)
                latest_success = checkpoint_df.filter(F.col("status") == "success")
                if latest_success.count() > 0:
                    return latest_success.select(_max("processed_version")).first()[0]
            return None
        except Exception as e:
            logger.warning(f"Error reading checkpoint: {str(e)}. Will process all available data.")
            return None

    def _get_staging_data(self, last_processed_version: Optional[int] = None) -> DataFrame:
        try:
            staging_delta_table = DeltaTable.forPath(self.spark, self.staging_path)
            current_version = staging_delta_table.history(1).select("version").collect()[0][0]

            if last_processed_version is None:
                staging_df = self.spark.read.format("delta").load(self.staging_path)
                logger.info(f"No checkpoint found. Reading all data up to version {current_version}")
            else:
                changes_df = self.spark.read.format("delta").option("readChangeFeed", "true") \
                    .option("startingVersion", last_processed_version + 1) \
                    .option("endingVersion", current_version).load(self.staging_path)

                logger.info(f"Reading changes from version {last_processed_version + 1} to {current_version}")
                staging_df = changes_df.filter(col("_change_type").isin(["insert", "update_postimage"]))

            self.current_staging_version = current_version
            logger.info(f"Loaded {staging_df.count()} records from staging")
            return staging_df
        except Exception as e:
            logger.error(f"Error reading staging data: {str(e)}")
            raise

    def _get_distinct_partitions(self, df: DataFrame) -> List[Dict[str, Any]]:
        distinct_parts = df.select("cc", "dd", "aa", "bb").distinct().toPandas()
        return distinct_parts.to_dict('records')

    def _process_partition(self, staging_df: DataFrame, partition_values: Dict[str, Any]) -> None:
        a, b, c, d = partition_values["aa"], partition_values["bb"], partition_values["cc"], partition_values["dd"]
        logger.info(f"Processing partition: aa={a}, bb={b}, cc={c}, dd={d}")

        filter_condition = (
            (col("aa") == a) & (col("bb") == b) &
            (col("cc") == c) & (col("dd") == d)
        )
        staging_part = staging_df.filter(filter_condition)

        if staging_part.count() == 0:
            logger.info(f"No data for partition: aa={a}, bb={b}, cc={c}, dd={d}. Skipping.")
            return

        staging_agg = (
            staging_part.groupBy("article_id", "article_version_id", "module_id")
            .agg(_sum("count").alias("view_count"))
            .withColumn("aa", lit(a))
            .withColumn("bb", lit(b))
            .withColumn("cc", lit(c))
            .withColumn("dd", lit(d))
        )

        staging_agg.createOrReplaceTempView("tmp_data")

        if not DeltaTable.isDeltaTable(self.spark, self.final_path):
            logger.info("Creating the final delta table")
            staging_agg.write.format("delta").mode("overwrite").partitionBy(
                "aa", "bb", "cc", "dd"
            ).save(self.final_path)
        else:
            try:
                merge_condition = """
                    
                """
                self.spark.sql(f"""
                
                """)
                logger.info(f"Successfully merged data for partition: aa={a}, bb={b}, cc={c}, dd={d}")
            except Exception as e:
                logger.error(f"Error during merge operation: {str(e)}")
                raise

    def _compact_partition(self, aa: Any, bb: Any, cc: Any, dd: Any) -> None:
        try:
            logger.info(f"Starting compaction for aa={aa}, bb={bb}, cc={cc}, dd={dd}")
            replace_condition = (
                (col("aa") == aa) & (col("bb") == bb) &
                (col("cc") == cc) & (col("dd") == dd)
            )
            df = self.spark.read.format("delta").load(self.final_path).filter(replace_condition)

            if df.count() == 0:
                logger.info(f"No data to compact for partition: aa={aa}, bb={bb}, cc={cc}, dd={dd}")
                return

            replace_where = f"aa = {aa} AND bb = {bb} AND cc = '{cc}' AND dd = '{dd}'"

            df.coalesce(1).write.format("delta").mode("overwrite") \
                .option("replaceWhere", replace_where) \
                .partitionBy("aa", "bb", "cc", "dd") \
                .save(self.final_path)

            logger.info(f"Completed compaction for partition: aa={aa}, bb={bb}, cc={cc}, dd={dd}")
        except Exception as e:
            logger.error(f"Error during compaction: {str(e)}")
            raise

    def run_pipeline(self) -> None:
        try:
            version, status, merged_parts = self._get_checkpoint_status()

            if status == "merged":
                logger.info("Detected partially completed version. Running compaction only.")
                for partition in merged_parts:
                    self._compact_partition(
                        aa=partition.aa,
                        bb=partition.bb,
                        cc=partition.cc,
                        dd=partition.dd
                    )
                self._finalize_checkpoint(version)
                logger.info("ETL pipeline compaction resumed and finalized.")
                return

            last_success_version = self._initialize_checkpoint()
            staging_df = self._get_staging_data(last_success_version)

            if staging_df.count() == 0:
                logger.info("No new data to process. Exiting.")
                return

            partitions = self._get_distinct_partitions(staging_df)
            logger.info(f"Found {len(partitions)} partitions to process")

            partition_objs = [PartitionInfo(**p) for p in partitions]

            for partition in partition_objs:
                self._process_partition(staging_df, asdict(partition))

            self._mark_version_merged(self.current_staging_version, partition_objs)

            for partition in partition_objs:
                self._compact_partition(
                    aa=partition.aa,
                    bb=partition.bb,
                    cc=partition.cc,
                    dd=partition.dd
                )

            self._finalize_checkpoint(self.current_staging_version)
            logger.info("ETL pipeline completed successfully")

        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise


def create_spark_session(app_name: str = "DeltaLakeETL") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main() -> None:
    staging_layer_path = x
    final_layer_path = b
    checkpoint_path = z

    spark = create_spark_session()
    etl = DeltaLakeETL(spark, staging_layer_path, final_layer_path, checkpoint_path)
    etl.run_pipeline()
    spark.stop()


if __name__ == "__main__":
    main()
