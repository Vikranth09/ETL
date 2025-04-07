from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import sum as _sum, lit, col, max as _max
from delta.tables import DeltaTable
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Any, Union

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ETLPipeline")

class DeltaLakeETL:
    """
    A class to handle ETL operations between staging and final Delta Lake tables.
    Implements incremental processing using checkpoint metadata.
    """
    
    def __init__(self, spark_session: SparkSession, staging_path: str, final_path: str, checkpoint_path: str) -> None:
        """
        Initialize the ETL class with paths and SparkSession.
        
        Args:
            spark_session: Active SparkSession
            staging_path: Path to staging Delta table
            final_path: Path to final Delta table
            checkpoint_path: Path to store checkpoint metadata
        """
        self.spark: SparkSession = spark_session
        self.staging_path: str = staging_path
        self.final_path: str = final_path
        self.checkpoint_path: str = checkpoint_path
        self.current_staging_version: Optional[int] = None
        
    def _initialize_checkpoint(self) -> Optional[int]:
        """
        Initialize or load checkpoint data to track processed files.
        
        Returns:
            Optional[int]: The latest processed version number or None if no checkpoint exists
        """
        # Check if checkpoint exists
        try:
            if DeltaTable.isDeltaTable(self.spark, self.checkpoint_path):
                checkpoint_df = self.spark.read.format("delta").load(self.checkpoint_path)
                # Get the latest processed version
                latest_version = checkpoint_df.select(_max("processed_version")).first()[0]
                logger.info(f"Found checkpoint with latest version: {latest_version}")
                return latest_version
            else:
                logger.info("No checkpoint found. Will process all available data.")
                return None
        except Exception as e:
            logger.warning(f"Error reading checkpoint: {str(e)}. Will process all available data.")
            return None
            
    def _update_checkpoint(self, current_version: int) -> None:
        """
        Update checkpoint with the latest processed version.
        
        Args:
            current_version: The current version number to save in the checkpoint
        """
        checkpoint_data: List[Dict[str, Union[int, str]]] = [
            {"processed_version": current_version, "update_time": datetime.now().isoformat()}
        ]
        checkpoint_df = self.spark.createDataFrame(checkpoint_data)
        
        # Write or update checkpoint
        if DeltaTable.isDeltaTable(self.spark, self.checkpoint_path):
            # Append to existing checkpoint
            checkpoint_df.write.format("delta").mode("append").save(self.checkpoint_path)
        else:
            # Create new checkpoint
            checkpoint_df.write.format("delta").mode("overwrite").save(self.checkpoint_path)
        
        logger.info(f"Updated checkpoint with version: {current_version}")
    
    def _get_staging_data(self, last_processed_version: Optional[int] = None) -> DataFrame:
        """
        Read staging data with optional filtering by last processed version.
        
        Args:
            last_processed_version: Delta table version to filter data after
            
        Returns:
            DataFrame with staging data
        """
        try:
            # Get the current version of the staging table
            staging_delta_table = DeltaTable.forPath(self.spark, self.staging_path)
            current_version = staging_delta_table.history(1).select("version").collect()[0][0]
            
            # If no checkpoint exists, read all data
            if last_processed_version is None:
                staging_df = self.spark.read.format("delta").load(self.staging_path)
                logger.info(f"No checkpoint found. Reading all data up to version {current_version}")
            else:
                # Read only the changes since the last processed version
                # Note: version + 1 because we want changes AFTER the last processed version
                changes_df = self.spark.read.format("delta").option(
                    "readChangeFeed", "true"
                ).option(
                    "startingVersion", last_processed_version + 1
                ).option(
                    "endingVersion", current_version
                ).load(self.staging_path)
                
                logger.info(f"Reading changes from version {last_processed_version + 1} to {current_version}")
                
                # Filter to only get inserts and updates (exclude deletes)
                staging_df = changes_df.filter(col("_change_type").isin(["insert", "update_postimage"]))
            
            # Save current version for the next checkpoint update
            self.current_staging_version = current_version
                
            logger.info(f"Loaded {staging_df.count()} records from staging")
            return staging_df
        except Exception as e:
            logger.error(f"Error reading staging data: {str(e)}")
            raise
    
    def _get_distinct_partitions(self, df: DataFrame) -> List[Dict[str, Any]]:
        """
        Extract distinct partition values from the staging data.
        
        Args:
            df: Staging DataFrame
            
        Returns:
            List of dictionaries with partition values
        """
        # Convert to Pandas for iteration
        # Note: This is safe as the distinct partitions should be small in number
        distinct_parts = df.select("p_split").distinct().toPandas()
        return distinct_parts.to_dict('records')
    
    def _process_partition(self, staging_df: DataFrame, partition_values: Dict[str, Any]) -> None:
        """
        Process a single partition of data.
        
        Args:
            staging_df: Staging DataFrame
            partition_values: Dictionary of partition values
        """
        a, b, c, d = (
            partition_values["a"], 
            partition_values["b"], 
            partition_values["c"], 
            partition_values["d"]
        )
        
        logger.info(f"Processing partition: a={a}, b={b}, c={c}, d={d}")
        
        # Filter staging data for this partition
        filter_condition = (
            (col("a") == y) &
            (col("b") == m) &
            (col("c") == c) &
            (col("d") == d)
        )
        
        staging_part = staging_df.filter(filter_condition)
        
        # Skip if no data for this partition
        if staging_part.count() == 0:
            logger.info(f"No data for partition: a={a}, b={b}, c={c}, d={d}. Skipping.")
            return
            
        # Aggregate staging data
        staging_agg = (
            staging_part.groupBy("article_id", "article_version_id", "module_id")
            .agg(_sum("count").alias("view_count"))
            .withColumn("a", lit(y))
            .withColumn("b", lit(m))
            .withColumn("c", lit(c))
            .withColumn("d", lit(d))
        )
        
        # Create temp view for merge operation
        staging_agg.createOrReplaceTempView("tmp_data")
        
        # Check if final Delta table exists
        deltatable_check = DeltaTable.isDeltaTable(self.spark, self.final_path)
        
        if not deltatable_check:
            # First time setup - create the table
            logger.info("Creating the final delta table")
            staging_agg.write.format("delta").mode("overwrite").partitionBy(
                "p_split"
            ).save(self.final_path)
        else:
            # Perform merge operation using SQL
            try:
                # Define the merge condition
                merge_condition = """
            
                """
                
                # Execute the merge operation using SQL
                self.spark.sql(f"""
                MERGE INTO delta.`{self.final_path}` AS target
                USING tmp_data AS source
                ON {merge_condition}
                WHEN MATCHED THEN UPDATE SET a.x = b.y
                WHEN NOT MATCHED THEN INSERT *
                """)
                
                logger.info(f"Successfully merged data for partition: a={a}, b={b}, c={c}, d={d}")
            except Exception as e:
                logger.error(f"Error during merge operation: {str(e)}")
                raise
                
        # Perform compaction for this partition
        self._compact_partition(a, b, c, d)
    
    def _compact_partition(self, a: Any, b: Any, c: Any, d: Any) -> None:
        """
        Compact a specific partition to optimize file size and performance.
        
       
        """
        try:
            logger.info(f"Starting compaction for a={a}, b={b}, c={c}, d={d}")
            
            # Define filter condition
            replace_condition = (
                (col("a") == a) & 
                (col("b") == b) &
                (col("c") == d) & 
                (col("d") == d)
            )
            
            # Read only the partition we want to compact
            df = self.spark.read.format("delta").load(self.final_path).filter(replace_condition)
            
            # Skip compaction if no data
            if df.count() == 0:
                logger.info(f"No data to compact for partition: a={a}, b={b}, c={c}, d={d}")
                return
                
            # Define the replaceWhere condition string
            replace_where = f"a = {a} AND b = {b} AND c = '{c}' AND d = '{d}'"
            
            # Perform compaction
            df.coalesce(1).write.format("delta").mode("overwrite") \
                .option("replaceWhere", replace_where) \
                .partitionBy("p_split) \
                .save(self.final_path)
                
            logger.info(f"Completed compaction for partition: a={a}, b={b}, c={c}, d={d}")
        except Exception as e:
            logger.error(f"Error during compaction: {str(e)}")
            raise
    
    def run_pipeline(self) -> None:
        """
        Main method to run the ETL pipeline.
        """
        try:
            # Get last processed version from checkpoint
            last_processed_version = self._initialize_checkpoint()
            
            # Get new staging data based on version
            # This method also sets self.current_staging_version
            staging_df = self._get_staging_data(last_processed_version)
            
            # If no new data, exit early
            if staging_df.count() == 0:
                logger.info("No new data to process. Exiting.")
                return
                
            # Get distinct partitions to process
            partitions = self._get_distinct_partitions(staging_df)
            logger.info(f"Found {len(partitions)} partitions to process")
            
            # Process each partition
            for partition in partitions:
                self._process_partition(staging_df, partition)
                
            # Update checkpoint with current version
            self._update_checkpoint(self.current_staging_version)
            
            logger.info("ETL pipeline completed successfully")
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise


def create_spark_session(app_name: str = "DeltaLakeETL") -> SparkSession:
    """
    Create and configure a Spark session.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        Configured SparkSession
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def main() -> None:
    """Main entry point for the ETL job."""
    # Define paths
    staging_layer_path = a
    final_layer_path = b
    checkpoint_path = c
    
    # Create Spark session
    spark = create_spark_session()
    
    # Initialize and run ETL pipeline
    etl = DeltaLakeETL(spark, staging_layer_path, final_layer_path, checkpoint_path)
    etl.run_pipeline()
    
    # Clean up
    spark.stop()


if __name__ == "__main__":
    main()