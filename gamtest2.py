from datetime import date ,datetime
import traceback
import pytz
import logging
import json
import pandas as pd
import  awswrangler as wr 
import time

#Slack is needed
from gamservices import GAMReportClient
from utils import setup_logging,get_env
from slack_msg_build import outer_user_block, outer_user_text_block, inner_info_block
from slack_notification import slack_notification, SlackAPI


logger= logging.getLogger(__name__)

def main():
    application_name =get_env("APPLICATION_NAME")
    network_code = get_env("NETWORK_CODE")
    service_account_json= get_env("SERVICE_ACCOUNT_JSON")
    google_ads_report_id = int(get_env("GOOGLE_ADS_REPORT_ID"))
    slack_bot_token = get_env("SLACK_BOT_TOKEN")
    imps_threshold = int(get_env("IMPS_THRESHOLD"))

    slack_webhook = get_env("SLACK_WEBHOOK")

    slack_api = SlackAPI(slack_bot_token)
    ENABLE_S3_STATE = False  # 👈 define FIRST
    aws_check_bucket = None
    if ENABLE_S3_STATE:
        aws_check_bucket = get_env("AWS_GEO_CHECK_BUCKET")
    sent_keys_df = pd.DataFrame(columns=["line_item_id", "creative_size"]) #I can remove if i need to fix s3 bucket using
    sent_keys_list = [] #I can remove if i need to fix s3 bucket using

    new_alerts_df = pd.DataFrame() #I can remove if i need to fix s3 bucket using




    logger.info(
        "Starting geo-check-alerts main: report_id=%s, imps_threshold=%d",
        google_ads_report_id,
        imps_threshold,
    )
    # Use a timezone-aware 'today' to ensure consistent filenames and ingestion timestamps
    today_date = datetime.now(pytz.timezone("America/New_York"))
    today_date_str = today_date.date().strftime("%Y-%m-%d")
    s3_file_path = None
    if ENABLE_S3_STATE:
        s3_file_path = (
            f"{aws_check_bucket if aws_check_bucket.endswith('/') else aws_check_bucket + '/'}"
            f"geo_sent_li_ids_{today_date_str}.csv"
        )

    #Parse service account json into dict for clinet  library usage .Keep the raw json out of logs
    with open(service_account_json, "r") as f:
        Service_account_dict = json.load(f)

    #Start GAM client using the provide service account object to avoid writing credetials to disk
    logger.debug("Intializing GAMREPORTclient ")
    client = GAMReportClient.from_service_account_obj(
        application_name= application_name,
        network_code= network_code,
        service_account_dict= Service_account_dict,
    )

    #Verify client credetials 
    client.check_client_service()
    logger.info("GAM clint verified and ready to work....")

    #Fetch the saved report defintion from gam.External api call
    logger.debug("Fetching the report from the gam api")
    report = client.get_saved_query(google_ads_report_id)

    #Force the report to use todays data so the job return only recent rows
    if hasattr(report, 'reportQuery'):
        print(f"reportQuery value: {report.reportQuery}")
    #report["reportQuery"]["dateRangeType"] ="TODAY"
    
    #Launch the report job  record the regturned job id for tracking
    logger.debug("Submitting report job to GAM API")
    if not hasattr(report, 'reportQuery') or report.reportQuery is None:
            print(" Error: This saved query has no valid report query definition.")
            return None
    report_job = client.run_report(report)
    report_job_id = report_job["id"]
    logger.info("Report job submitted: job_id=%s", report_job_id)

    # Wait for report completion and read into a DataFrame. This may block on GAM API.
    delivery_df = client.fetch_report_df(report_job_id)
    violations = []




#----Report pulling csv------QA
#     path ="test.csv"

#     delivery_df.to_csv(path)
#     return delivery_df
# if __name__ == "__main__":
#     df = main() 
#     if df is not None:
#         print(df.head())
#------Report pulling csv -----QA

    #Core logic 
    delivery_df["video_viewership_video_length"] = pd.to_numeric(
    delivery_df["video_viewership_video_length"].replace("-", 0),
    errors="coerce"
    )

    delivery_df["video_viewership_skip_button_shown"] = pd.to_numeric(
        delivery_df["video_viewership_skip_button_shown"].replace("-", 0),
        errors="coerce"
    )

    delivery_df["programmatic_deal_id"] = pd.to_numeric(
        delivery_df["programmatic_deal_id"],
        errors="coerce"
    )     
    delivery_df.to_csv("test.csv", index=False)
    return delivery_df
if __name__ == "__main__":
    df = main() 
    if df is not None:
        print(df.head())