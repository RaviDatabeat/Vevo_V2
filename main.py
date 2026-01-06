# ///script
# requires-python = ">=3.11"
# dependencies = [
#     "awswrangler>=3.14.0",
#     "googleads>=48.0.0",
#     "pandas>=2.3.3",
# ]
# ///


from datetime import datetime
import traceback
import pytz
import logging
import json 
import os


import pandas as pd
import awswrangler as wr
import boto3

from gamservices import GAMReportClient
from utils import setup_logging,get_env
from slack_msg_build import outer_user_block, outer_user_text_block, inner_info_block
from slack_notification import slack_notification, SlackAPI

logger= logging.getLogger(__name__)

aws_profile = get_env("AWS_PROFILE")
boto3_session = boto3.Session(profile_name=aws_profile)

def run_report(client, report_id: int) -> pd.DataFrame:
    logger.info("Running GAM report_id=%s", report_id)

    report = client.get_saved_query(report_id)
    
    # Check if report exists and has a valid reportQuery
    if not report or not hasattr(report, 'reportQuery') or report.reportQuery is None:
        logger.error("Saved report is empty or missing reportQuery for report_id=%s", report_id)
        logger.error("Report object: %s", report)
        return pd.DataFrame()  # Return empty DataFrame
    
    logger.debug("Fetched report object: %s", report)
    job = client.run_report(report)
    report_job_id = job["id"]

    return client.fetch_report_df(report_job_id)

def normalize_delivery_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["video_viewership_video_length"] = pd.to_numeric(
        df["video_viewership_video_length"].replace("-", 0),
        errors="coerce"
    )

    df["video_viewership_skip_button_shown"] = pd.to_numeric(
        df["video_viewership_skip_button_shown"].replace("-", 0),
        errors="coerce"
    )



    return df
def apply_skip_rule1(df: pd.DataFrame) -> pd.DataFrame:
    return df[
        (df["video_viewership_video_length"].round(0) > 16) & #16
        (df["creative_size"] == "480 x 360v") &
        (
            ~df["creative_name"]
            .str.contains("Non-Skip Video Ad 30s", case=False, na=False)
        )
    ].copy()
def apply_skip_rule2(df: pd.DataFrame) -> pd.DataFrame:
    return df[
        (df["video_viewership_video_length"].round(0) > 21) & #21
        (df["creative_size"] == "480 x 360v") &
        (
            ~df["creative_name"]
            .str.contains("Non-Skip Video Ad 30s", case=False, na=False)
        )
    ].copy()

def read_s3_state(s3_path: str) -> pd.DataFrame:
    """
    Read the S3 CSV that tracks previously alerted items.
    Returns an empty DataFrame if the file doesn't exist.
    """
    try:
        df = wr.s3.read_csv(s3_path,  boto3_session=boto3_session)
        # return pd.read_csv(s3_path) #For test

        return df
    except Exception as e:
        logger.info("No existing S3 dataset found at %s: %s", s3_path, e)
        return pd.DataFrame(columns=["line_item_id", "creative_name"])

def write_s3_state(df: pd.DataFrame, s3_path: str):
    """
    Save the updated state to S3.
    """
    wr.s3.to_csv(
    df.drop_duplicates(),
    s3_path,
    index=False,
    boto3_session=boto3_session
)

    # df.drop_duplicates().to_csv(s3_path, index=False) #For test
    logger.info("Updated S3 state at %s with %d keys", s3_path, len(df))

def process_alerts(
    final_df: pd.DataFrame,
    s3_dataset_path: str,
    slack_api: SlackAPI,
    slack_webhook: str,
    title: str,
):
    # 1️ No violations → do NOTHING
    if final_df.empty:
        logger.info("No violations for %s", title)
        return

    # 2️ Read previously alerted keys
    sent_keys_df = read_s3_state(s3_dataset_path)

    sent_keys_set = set(
        zip(
            sent_keys_df["line_item_id"].astype(str),
            sent_keys_df["creative_name"].astype(str),
        )
    )

    # 3️ Build key for current violations
    final_df = final_df.copy()
    final_df["key_tuple"] = list(
        zip(
            final_df["line_item_id"].astype(str),
            final_df["creative_name"].astype(str),
        )
    )

    # 4️ Filter ONLY new violations
    new_alerts_df = final_df[
        ~final_df["key_tuple"].isin(sent_keys_set)
    ]
    if not new_alerts_df.empty:
        new_alerts_df = new_alerts_df.copy()
        new_alerts_df["alert_date"] = datetime.now(pytz.timezone("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")

    # No new violations → NO Slack
    if new_alerts_df.empty:
        logger.info("No NEW alerts for %s", title)
        return

    # 5Build Slack message (ONLY for new violations)
    elements = []
    for order_trafficker, grouped_df in new_alerts_df.groupby("order_trafficker"):
        try:
            user_email = order_trafficker.split("(")[1].split(")")[0]
        except Exception:
            user_email = order_trafficker

        user_id = slack_api.lookup_by_email(user_email)
        if user_id:
            elements.append(outer_user_block(user_id))
        else:
            elements.append(outer_user_text_block(user_email))

        elements.append(inner_info_block(grouped_df))
        elements.append(
            {"type": "rich_text_section", "elements": [{"type": "text", "text": "\n"}]}
        )

    blocks = [
    {
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": title
        }
    },
    {
        "type": "section",
        "text": {
            "type": "plain_text",
            "text": f"The following line items require immediate attention as they are over max duration."
        }
    },
    {"type": "divider"},
    {"type": "rich_text", "elements": elements},
    ]


    logger.info("Sending Slack alert for %s", title)
    slack_notification(slack_webhook, {"blocks": blocks})

    # 6️ Update state AFTER  (critical)
    updated_state_df = pd.concat(
    [
        sent_keys_df,
        new_alerts_df[["line_item_id", "creative_name", "alert_date"]].astype(str),
    ],
    ignore_index=True,
    ).drop_duplicates()

    write_s3_state(updated_state_df, s3_dataset_path)




def main():
    application_name =get_env("APPLICATION_NAME")
    network_code = get_env("NETWORK_CODE")
    service_account_json= get_env("SERVICE_ACCOUNT_JSON")
    google_ads_report_id1 = int(get_env("GOOGLE_ADS_REPORT_ID1"))
    google_ads_report_id2= int(get_env("GOOGLE_ADS_REPORT_ID2"))

    slack_bot_token = get_env("SLACK_BOT_TOKEN")

    slack_webhook = get_env("SLACK_WEBHOOK")

    slack_api = SlackAPI(slack_bot_token)
    aws_skip_check_bucket16 = get_env("AWS_SKIP_CHECK_BUCKET16")
    aws_skip_check_bucket21 = get_env("AWS_SKIP_CHECK_BUCKET21")

    logger.info(
       "Starting skip_not_enabled-check main: report_id1=%s report_id2=%s",
        google_ads_report_id1,
        google_ads_report_id2,
    )
    # Use a timezone-aware 'today' to ensure consistent filenames and ingestion timestamps
    today_date = datetime.now(pytz.timezone("America/New_York"))
    today_date_str = today_date.date().strftime("%Y-%m-%d")
    s3_dataset_path_16s = (
    aws_skip_check_bucket16.rstrip("/") + f"/skip_not_enabled_16s/date={today_date_str}.csv"
)

    s3_dataset_path_21s = (
        aws_skip_check_bucket21.rstrip("/") + f"/skip_not_enabled_21s/date={today_date_str}.csv"
    )

    # s3_dataset_path_16s = f"./skip_not_enabled_16s_{today_date_str}.csv" #Test
    # s3_dataset_path_21s = f"./skip_not_enabled_21s_{today_date_str}.csv"#Test
    logger.debug("S3 state file path resolved: %s", s3_dataset_path_16s)
    logger.debug("S3 state file path resolved: %s", s3_dataset_path_21s)

 

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
    delivery_df1 = run_report(client, google_ads_report_id1)
    delivery_df2 = run_report(client, google_ads_report_id2)
    delivery_df1.to_csv("16secreport.csv", index= False)
    delivery_df2.to_csv("21secreport.csv", index=False)
    print()
    # Check if DataFrames are not empty before processing
    if delivery_df1.empty:
        logger.warning("Report 1 returned empty DataFrame. Skipping processing.")
        violations_df1 = pd.DataFrame()
    else:
        delivery_df1 = normalize_delivery_df(delivery_df1)
        violations_df1 = apply_skip_rule1(delivery_df1)



    if delivery_df2.empty:
        logger.warning("Report 2 returned empty DataFrame. Skipping processing.")
        violations_df2 = pd.DataFrame()
    else:
        delivery_df2 = normalize_delivery_df(delivery_df2)
        violations_df2 = apply_skip_rule2(delivery_df2)

    df_skip_rule1 = apply_skip_rule1(delivery_df1)  # Test file for meta data iam using we can  rempve after wards
    df_skip_rule2 = apply_skip_rule2(delivery_df2)

    # Save the returned DataFrames to CSV
    df_skip_rule1.to_csv("Voilation16secreport.csv", index=False)
    df_skip_rule2.to_csv("Voilation21secreport.csv", index=False)



    #Core logic 
    # delivery_df1 = normalize_delivery_df(delivery_df1)
    # delivery_df2 = normalize_delivery_df(delivery_df2)
     
    # violations_df1 = apply_skip_rule1(delivery_df1)
    # violations_df2 = apply_skip_rule2(delivery_df2)  #After qa i will add this logic

    #To check weater creative size is unique 
    print(violations_df1["creative_size"].unique())
    print(violations_df2["creative_size"].unique())

    process_alerts(
    final_df=violations_df1,
    s3_dataset_path=s3_dataset_path_16s,
    slack_api=slack_api,
    slack_webhook=slack_webhook,
    title="16s Skip Not Enabled Alert"
    )

    process_alerts(
        final_df=violations_df2,
        s3_dataset_path=s3_dataset_path_21s,
        slack_api=slack_api,
        slack_webhook=slack_webhook,
        title="21s Skip Not Enabled Alert"
    )

    

if __name__ == "__main__":
    import os
    import awswrangler as wr
    from slack_notification import simple_slack_notification

    aws_profile = get_env("AWS_PROFILE")
    boto3_session = boto3.Session(profile_name=aws_profile)
    status_slack_webhook = get_env("STATUS_SLACK_WEBHOOK")

    simple_slack_notification(
        status_slack_webhook,
        "ad-ops-duration-to-ad-product-check-Alert Started!",
    )

    try:
        setup_logging()

        main()
    except Exception as e:
        logging.error(f"Uncaught exception: {e}")
        logging.error(traceback.format_exc())
        simple_slack_notification(
            status_slack_webhook,
            f"🚨🚨 Skip_enabled Miss-check-alert failed! 🚨🚨\nUncaught exception: {e}",
        )
    finally:
        try:
            import os
        

            bucket = os.getenv("AWS_LOG_BUCKET")
            now = datetime.now()
            log_key = (
                f"s3://{bucket}/logs/"
                f"vedio-country-error-check.log{now.strftime('%Y-%m-%d_%H-%M-%S')}.log"
            )

            local_log_file = "video-skip-enabled-error-check.log"
            wr.s3.upload(local_log_file, log_key,  boto3_session=boto3_session)
            print(f"✅ Log uploaded to {log_key}")
            print(f"✅ Log uploaded to {log_key}")
            simple_slack_notification(
                status_slack_webhook,
                f"ad-ops-duration-country-check!\n✅ Log uploaded to {log_key}",
            )
        except Exception as upload_err:
            # if upload fails, at least print to stdout
            simple_slack_notification(
                status_slack_webhook, f"⚠️ Failed to upload log to S3: {upload_err}"
            )
