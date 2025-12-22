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
    send_result = None  # Initialize early to avoid UnboundLocalError
    application_name =get_env("APPLICATION_NAME")
    network_code = get_env("NETWORK_CODE")
    service_account_json= get_env("SERVICE_ACCOUNT_JSON")
    google_ads_report_id = int(get_env("GOOGLE_ADS_REPORT_ID"))
    slack_bot_token = get_env("SLACK_BOT_TOKEN")
    imps_threshold = int(get_env("IMPS_THRESHOLD"))

    slack_webhook = get_env("SLACK_WEBHOOK")

    slack_api = SlackAPI(slack_bot_token)
    ENABLE_S3_STATE = False  # define FIRST
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
    report.reportQuery["dateRangeType"] ="TODAY"
    
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
    rule_violation = delivery_df[  # Use df directly (already filtered)
        (delivery_df["video_viewership_video_length"] >= 30) &
        (delivery_df["video_viewership_skip_button_shown"] == 0) & 
        (delivery_df["creative_size"] == "480 x 361v" ) & 
        (delivery_df["programmatic_deal_id"] == 0)
            ]
    #To check weater creative size is unique 
    print(delivery_df["creative_size"].unique())

    if not rule_violation.empty:
        rule_violation = rule_violation.copy()
        logger.info("Violation found")
        violations.append(rule_violation)
    if violations:
        final_df = pd.concat(violations, ignore_index=True)
        # print(final_df)
        final_df.to_csv("rule_violations.csv", index=False)

#----Report pulling csv------QA
#     path ="test.csv"

#     delivery_df.to_csv(path)
#     return delivery_df
# if __name__ == "__main__":
#     df = main() 
#     if df is not None:
#         print(df.head())
#----Report pulling csv------QA
     # Track which line items we've previously alerted on to avoid duplicate notifications
    # sent_li_id_list = []
      # Use S3 as a simple state store: read previous alerted line items if file exists
        # Use S3 as a simple state store: read previous alerted line items if file exists.
    # This lets the job avoid duplicate Slack notifications across runs on the same day.
    # ... [your existing code until line 89] ...

    # Use S3 as a simple state store: read previous alerted line items if file exists.
    # This lets the job avoid duplicate Slack notifications across runs on the same day.
    # Use S3 as a simple state store
    if ENABLE_S3_STATE:
        try:
            if wr.s3.does_object_exist(s3_file_path):
                logger.info("Found existing S3 state file: %s", s3_file_path)
                sent_keys_df = wr.s3.read_csv(s3_file_path)  # Renamed variable
                
                # Create list of tuples (line_item_id, creative_size)
                sent_keys_list = list(set(
                    zip(sent_keys_df["line_item_id"].astype(str), 
                        sent_keys_df["creative_size"].astype(str))
                ))
                logger.info("Loaded %d previously alerted keys", len(sent_keys_list))
            else:
                logger.info("No existing S3 state file found")
                sent_keys_df = pd.DataFrame(columns=['line_item_id', 'creative_size'])
        except Exception as e:
            logger.error("Failed to read S3 state file: %s", e)
            sent_keys_df = pd.DataFrame(columns=['line_item_id', 'creative_size'])

    # Process violations if any were found
    
    if violations:
        final_df = pd.concat(violations, ignore_index=True)
        
        # Create key tuple for each violation
        final_df["key_tuple"] = list(zip(
            final_df["line_item_id"].astype(str),
            final_df["creative_size"].astype(str)
        ))
        
        # Check if this (line_item_id, creative_size) was already alerted
        final_df["previous_alert_status"] = final_df["key_tuple"].isin(sent_keys_list)
        
        # Create DataFrame with both columns for S3
        all_violation_keys = pd.DataFrame({
            'line_item_id': final_df['line_item_id'].astype(str),
            'creative_size': final_df['creative_size'].astype(str)
        })
        # statepath = f"geo_sent_li_ids_{today_date_str}.csv" ---

        #To define for testing in place of using in aws s3 bucket function
    
        sent_keys_df = pd.DataFrame(columns=["line_item_id", "creative_size"])
        sent_keys_list = []

        # Combine with previous keys and save
        new_state_df = pd.concat([sent_keys_df, all_violation_keys]).drop_duplicates()
        # new_state_df.to_csv(statepath, index=False)
        if ENABLE_S3_STATE:
            try:
                wr.s3.to_csv(new_state_df, s3_file_path, index=False)
                logger.info("Updated S3 state with %d new keys", len(all_violation_keys))
            except Exception as e:
                logger.error("Failed to write to S3: %s", e)
        
        # Filter for NEW alerts
        new_alerts_df = final_df[~final_df["previous_alert_status"]].copy(deep=True)
        
        if new_alerts_df.empty:
            logger.info("No NEW alerts (all were previously alerted)")
            return
        else:
            logger.info("Found %d NEW alerts", len(new_alerts_df))
    # Build Slack blocks for only the alerts that haven't been sent previously.
    # Group alerts by `order_trafficker` to direct messages to the right users.
            elements = []
            print(new_alerts_df.columns.tolist())
            li_group_df = new_alerts_df.groupby(["order_trafficker"])

            for i, grouped_df in li_group_df:
                # `i` is the group key (order_trafficker). Extract the email address from the stored string.
                user_email_raw: str = i[0]
                # The saved format is expected to include the email in parentheses (e.g., "Name (email)").
                # We defensively parse this and fall back to the raw string if format differs.
                if "(" in user_email_raw and ")" in user_email_raw:
                    user_email = user_email_raw.split("(")[1].split(")")[0]
                else:
                    user_email = user_email_raw.strip()  # fallback to raw string

                # Lookup Slack user
                user_id = slack_api.lookup_by_email(user_email)

                if user_id:
                    # Slack user exists → tag
                    elements.append(outer_user_block(user_id))
                else:
                    # Slack user not found → show email
                    logger.warning(
                        "Slack user not found for email %s, using email in message", user_email
                    )
                    elements.append(outer_user_text_block(user_email))

                # Add grouped alert details
                elements.append(inner_info_block(grouped_df))
                elements.append({"type": "rich_text_section", "elements": [{"type": "text", "text": "\n"}]})

                # Rate-limit lookups/requests to avoid hitting Slack API rate limits.
                time.sleep(2)
            blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": " Creative size Skip not enabled alert ",
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "plain_text",
                        "text": r"The following line items require immediate attention due to a skip not enabled for creative  vedio duration >= 30 sec:",
                    },
                },
                {"type": "divider"},
                {"type": "rich_text", "elements": elements},
            ]

            json_msg = {"blocks": blocks}

            # Send Slack notification and log the outcome. Avoid logging the webhook URL itself.
            logger.info(
                "Sending Slack notification. Result: %s, report_id=%s",
                str(send_result),
                google_ads_report_id,
            )
            # Send Slack notification via incoming webhook. We intentionally avoid logging the webhook URL.
            try:
                slack_webhook = get_env("SLACK_WEBHOOK")
            except ValueError:
                slack_webhook = None
                logger.warning("SLACK_WEBHOOK not set — Slack disabled")
    else:
        logger.info("No violations found today. Sending NAA Slack message.")
        json_msg = {
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "✅ No Creative Size violations today."}
                }
            ]
        }
        
    try:
        send_result = slack_notification(slack_webhook, json_msg)
        logger.info(
            "Slack notification sent. Result: %s", str(send_result)
        )
    except Exception as e:
        logger.exception("Failed to send Slack notification: %s", e)



if __name__ == "__main__":
    setup_logging()

    main()
    # import awswrangler as wr
    # from slack_notification import simple_slack_notification

    # status_slack_webhook = get_env("STATUS_SLACK_WEBHOOK")

    # simple_slack_notification(
    #     status_slack_webhook,
    #     "Creativesize-errors-alert Started!",
    # )

    # try:
    #     setup_logging()

    #     main()
    # except Exception as e:
    #     logging.error(f"Uncaught exception: {e}")
    #     logging.error(traceback.format_exc())
    #     simple_slack_notification(
    #         status_slack_webhook,
    #         f"🚨🚨 Creativesize Miss-check-alert failed! 🚨🚨\nUncaught exception: {e}",
    #     )
    # finally:
    #     try:
    #         import os
        

    #         bucket = os.getenv("AWS_LOG_BUCKET")
    #         now = datetime.now()
    #         log_key = f"s3://{bucket}/logs/Creativesize Miss-check-alert-{now.strftime('%Y-%m-%d %H:%M:%S')}.log"
    #         wr.s3.upload(
    #             "Creativesize Miss-check-alert.log", log_key
    #         )  # upload log file to s3
    #         print(f"✅ Log uploaded to {log_key}")
    #         simple_slack_notification(
    #             status_slack_webhook,
    #             f"Creativesize Miss-alert completed!\n✅ Log uploaded to {log_key}",
    #         )
    #     except Exception as upload_err:
    #         # if upload fails, at least print to stdout
    #         simple_slack_notification(
    #             status_slack_webhook, f"⚠️ Failed to upload log to S3: {upload_err}"
    #         )