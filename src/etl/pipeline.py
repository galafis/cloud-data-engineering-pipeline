"""
Cloud Data Engineering Pipeline

ETL pipeline for cloud data processing with AWS/GCP integration.

Author: Gabriel Demetrios Lafis
"""

import pandas as pd
import boto3
from typing import Dict, List, Optional
from loguru import logger
from datetime import datetime


class CloudDataPipeline:
    """
    Cloud data engineering pipeline for ETL operations.
    """
    
    def __init__(
        self,
        cloud_provider: str = 'aws',
        bucket_name: str = None,
        region: str = 'us-east-1'
    ):
        """
        Initialize cloud data pipeline.
        
        Args:
            cloud_provider: Cloud provider ('aws' or 'gcp')
            bucket_name: S3 bucket or GCS bucket name
            region: Cloud region
        """
        self.cloud_provider = cloud_provider
        self.bucket_name = bucket_name
        self.region = region
        
        if cloud_provider == 'aws':
            self.s3_client = boto3.client('s3', region_name=region)
            logger.info(f"Initialized AWS pipeline with bucket: {bucket_name}")
        elif cloud_provider == 'gcp':
            from google.cloud import storage
            self.gcs_client = storage.Client()
            self.bucket = self.gcs_client.bucket(bucket_name)
            logger.info(f"Initialized GCP pipeline with bucket: {bucket_name}")
        else:
            raise ValueError(f"Unknown cloud provider: {cloud_provider}")
    
    def extract_from_s3(self, key: str) -> pd.DataFrame:
        """
        Extract data from S3.
        
        Args:
            key: S3 object key
            
        Returns:
            DataFrame with extracted data
        """
        logger.info(f"Extracting data from s3://{self.bucket_name}/{key}")
        
        obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        
        if key.endswith('.csv'):
            df = pd.read_csv(obj['Body'])
        elif key.endswith('.parquet'):
            df = pd.read_parquet(obj['Body'])
        elif key.endswith('.json'):
            df = pd.read_json(obj['Body'])
        else:
            raise ValueError(f"Unsupported file format: {key}")
        
        logger.success(f"Extracted {len(df)} rows")
        return df
    
    def transform(
        self,
        df: pd.DataFrame,
        transformations: List[Dict] = None
    ) -> pd.DataFrame:
        """
        Transform data.
        
        Args:
            df: Input DataFrame
            transformations: List of transformation configurations
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Applying transformations...")
        
        df_transformed = df.copy()
        
        if transformations:
            for transform in transformations:
                operation = transform.get('operation')
                
                if operation == 'drop_nulls':
                    df_transformed = df_transformed.dropna(
                        subset=transform.get('columns', None)
                    )
                
                elif operation == 'fill_nulls':
                    df_transformed = df_transformed.fillna(
                        transform.get('value', 0)
                    )
                
                elif operation == 'rename':
                    df_transformed = df_transformed.rename(
                        columns=transform.get('mapping', {})
                    )
                
                elif operation == 'filter':
                    condition = transform.get('condition')
                    df_transformed = df_transformed.query(condition)
                
                elif operation == 'aggregate':
                    group_by = transform.get('group_by', [])
                    agg_dict = transform.get('aggregations', {})
                    df_transformed = df_transformed.groupby(group_by).agg(agg_dict).reset_index()
                
                elif operation == 'add_column':
                    col_name = transform.get('name')
                    col_value = transform.get('value')
                    df_transformed[col_name] = col_value
                
                elif operation == 'convert_type':
                    col_name = transform.get('column')
                    dtype = transform.get('dtype')
                    df_transformed[col_name] = df_transformed[col_name].astype(dtype)
        
        logger.success(f"Transformation complete. Output: {len(df_transformed)} rows")
        return df_transformed
    
    def load_to_s3(
        self,
        df: pd.DataFrame,
        key: str,
        format: str = 'parquet'
    ):
        """
        Load data to S3.
        
        Args:
            df: DataFrame to load
            key: S3 object key
            format: Output format ('parquet', 'csv', 'json')
        """
        logger.info(f"Loading data to s3://{self.bucket_name}/{key}")
        
        if format == 'parquet':
            buffer = df.to_parquet(index=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=buffer
            )
        elif format == 'csv':
            buffer = df.to_csv(index=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=buffer
            )
        elif format == 'json':
            buffer = df.to_json(orient='records')
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=buffer
            )
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.success(f"Loaded {len(df)} rows to S3")
    
    def run_etl_pipeline(
        self,
        source_key: str,
        destination_key: str,
        transformations: List[Dict] = None,
        output_format: str = 'parquet'
    ) -> Dict:
        """
        Run complete ETL pipeline.
        
        Args:
            source_key: Source data key
            destination_key: Destination data key
            transformations: List of transformations
            output_format: Output format
            
        Returns:
            Dictionary with pipeline statistics
        """
        start_time = datetime.now()
        
        logger.info("Starting ETL pipeline...")
        
        # Extract
        df = self.extract_from_s3(source_key)
        input_rows = len(df)
        
        # Transform
        df_transformed = self.transform(df, transformations)
        output_rows = len(df_transformed)
        
        # Load
        self.load_to_s3(df_transformed, destination_key, output_format)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        stats = {
            'input_rows': input_rows,
            'output_rows': output_rows,
            'duration_seconds': duration,
            'rows_per_second': output_rows / duration if duration > 0 else 0,
            'source': source_key,
            'destination': destination_key
        }
        
        logger.success(f"ETL pipeline complete in {duration:.2f}s")
        logger.info(f"Pipeline stats: {stats}")
        
        return stats
    
    def validate_data_quality(
        self,
        df: pd.DataFrame,
        rules: List[Dict]
    ) -> Dict:
        """
        Validate data quality.
        
        Args:
            df: DataFrame to validate
            rules: List of validation rules
            
        Returns:
            Dictionary with validation results
        """
        logger.info("Validating data quality...")
        
        results = {
            'passed': [],
            'failed': [],
            'total_rules': len(rules)
        }
        
        for rule in rules:
            rule_type = rule.get('type')
            
            if rule_type == 'not_null':
                column = rule.get('column')
                null_count = df[column].isnull().sum()
                if null_count == 0:
                    results['passed'].append(rule)
                else:
                    rule['error'] = f"{null_count} null values found"
                    results['failed'].append(rule)
            
            elif rule_type == 'unique':
                column = rule.get('column')
                duplicate_count = df[column].duplicated().sum()
                if duplicate_count == 0:
                    results['passed'].append(rule)
                else:
                    rule['error'] = f"{duplicate_count} duplicates found"
                    results['failed'].append(rule)
            
            elif rule_type == 'range':
                column = rule.get('column')
                min_val = rule.get('min')
                max_val = rule.get('max')
                out_of_range = df[(df[column] < min_val) | (df[column] > max_val)]
                if len(out_of_range) == 0:
                    results['passed'].append(rule)
                else:
                    rule['error'] = f"{len(out_of_range)} values out of range"
                    results['failed'].append(rule)
        
        results['pass_rate'] = len(results['passed']) / len(rules) if rules else 0
        
        logger.info(f"Data quality validation: {len(results['passed'])}/{len(rules)} rules passed")
        
        return results


if __name__ == "__main__":
    # Example usage (requires AWS credentials)
    
    # Initialize pipeline
    pipeline = CloudDataPipeline(
        cloud_provider='aws',
        bucket_name='my-data-bucket',
        region='us-east-1'
    )
    
    # Define transformations
    transformations = [
        {'operation': 'drop_nulls', 'columns': ['id', 'value']},
        {'operation': 'filter', 'condition': 'value > 0'},
        {'operation': 'add_column', 'name': 'processed_date', 'value': datetime.now().strftime('%Y-%m-%d')}
    ]
    
    # Run ETL pipeline
    # stats = pipeline.run_etl_pipeline(
    #     source_key='raw/data.csv',
    #     destination_key='processed/data.parquet',
    #     transformations=transformations
    # )
    
    print("Cloud Data Pipeline initialized successfully")
