import logging
import logging.config
import yaml


"""Running the Xetra ETL Pipeline"""
def main():
    """passing ynal file"""
    config_path = "C:\\Users\\Sam\\OneDrive\\Desktop\\xetra_project\\xetra_Fezi\\configs\\xetra_reportt1.config.yml"
    config=yaml.safe_load(open(config_path))
    """config logging"""
    log_config=config['logging']
    logging.config.dictConfig(log_config)
    logger=logging.getLogger(__name__)
    logger.info("this is a test")
    
if __name__=='__main__':
    main()
