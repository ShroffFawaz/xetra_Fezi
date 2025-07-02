import logging
import logging.config
import yaml
import argparse

from xetra.common.s3 import s3Bucketconncetor
from xetra.transformers.xetra_transformation import XetraETL,XetraSourceCofig,XetraTargetConfig

"""Running the Xetra ETL Pipeline"""
def main():
    """passing ynal file"""
    parser=argparse.ArgumentParser(description="Run the xetra ETL job")
    parser.add_argument('config',help="A configuration file in YAML format")
    args=parser.parse_args()
    config=yaml.safe_load(open(args.config))
    """config logging"""
    log_config=config['logging']
    logging.config.dictConfig(log_config)
    logger=logging.getLogger(__name__)
    #Reading s3 configuration
    s3_config=config['s3']    
    #Creating the s3BucketConncetor class instances for source and target 
    s3_bucket_src=s3Bucketconncetor(access_key=s3_config['access_key'],
                                    secret_key=s3_config['secret_key'],
                                    endpoint_url=s3_config['src_endpoint_url'],
                                    bucket=s3_config['src_bucket'])
    s3_bucket_trg=s3Bucketconncetor(access_key=s3_config['access_key'],
                                    secret_key=s3_config['secret_key'],
                                    endpoint_url=s3_config['trg_endpoint_url'],
                                    bucket=s3_config['trg_bucket'])
    #Reading source configuration
    source_config=XetraSourceCofig(**config['source'])
    #Reading target configuration
    target_config=XetraTargetConfig(**config['target'])
    #Reading meta configuaration
    meta_config=config['meta']
    #Creating xetraETL class instance
    logger.info("Xetra ETL job started")
    xetra_etl=XetraETL(s3_bucket_src,s3_bucket_trg,
                       meta_config['meta_key'],
                       source_config,target_config, meta_config['meta_update_list'],
                       meta_config['extract_date_list'],meta_config['extract_date'])
    # running ETL job for xetra report1
    xetra_etl.etl_report1()
    logger.info("Xetra ETL job finished")

if __name__=='__main__':
    main()
