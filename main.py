from datetime import date ,datetime
import traceback
import pytz
import logging
import json
import pandas as pd
import awswrangler as wr 
import time
from dotenv import load_dotenv
import os
import pprint
load_dotenv() 
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

    slack_webhook = get_env("SLACK_WEBHOOK")

    slack_api = SlackAPI(slack_bot_token)
    aws_skip_check_bucket = get_env("AWS_SKIP_CHECK_BUCKET")





    logger.info(
        "Starting skip_not_enabled-check main: report_id=%s",
        google_ads_report_id,
    )
    # Use a timezone-aware 'today' to ensure consistent filenames and ingestion timestamps
    today_date = datetime.now(pytz.timezone("America/New_York"))
    today_date_str = today_date.date().strftime("%Y-%m-%d")

    s3_dataset_path = (
    aws_skip_check_bucket.rstrip("/")
    + f"/skip_not_enabled/date={today_date_str}.csv"
)
    logger.debug("S3 state file path resolved: %s", s3_dataset_path)
 

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


    print(f"reportQuery value: {report.reportQuery}")
    
    #Launch the report job  record the regturned job id for tracking
    logger.debug("Submitting report job to GAM API")

    report_job = client.run_report(report)
    report_job_id = report_job["id"]
    logger.info("Report job submitted: job_id=%s", report_job_id)

    # Wait for report completion and read into a DataFrame. This may block on GAM API.
    delivery_df = client.fetch_report_df(report_job_id)



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
    rule_violation = delivery_df[  # Use df directly (already filtered)
        (delivery_df["video_viewership_video_length"] >= 30) &
        (delivery_df["video_viewership_skip_button_shown"] == 0) & 
        (delivery_df["creative_size"] == "480 x 361v" ) & 
        (delivery_df["programmatic_deal_id"] == 0)
            ]
    #To check weater creative size is unique 
    print(delivery_df["creative_size"].unique())
    if rule_violation.empty:
        logger.info("No violations found")
        return

    final_df = rule_violation.copy()
    logger.info("Violation found")

        
     # Track which line items we've previously alerted on to avoid duplicate notifications
    # This lets the job avoid duplicate Slack notifications across runs on the same day.
    # Use S3 as a simple state store
    # sent_keys_df = pd.DataFrame(columns=['line_item_id', 'creative_name'])
    sent_keys_list = []


    try:
        sent_keys_df = wr.s3.read_csv(s3_dataset_path)
        if sent_keys_df.empty:
            logger.info("S3 dataset exists but no records for date=%s", today_date_str)
            sent_keys_list = []
        elif "creative_name" in sent_keys_df.columns:
            sent_keys_list = list(set(
                zip(
                    sent_keys_df["line_item_id"].astype(str),
                    sent_keys_df["creative_name"].astype(str)
                )
            ))
            logger.info("Loaded %d previously alerted keys (creative_name)", len(sent_keys_list))

        elif "creative_size" in sent_keys_df.columns:
            sent_keys_list = list(set(
                zip(
                    sent_keys_df["line_item_id"].astype(str),
                    sent_keys_df["creative_size"].astype(str)
                )
            ))
            logger.info("Loaded %d previously alerted keys (creative_size)", len(sent_keys_list))

    except Exception as e:
        logger.info(
            "No existing S3 dataset/partition found for date=%s (%s)",
            today_date_str,
            str(e)
        )
        sent_keys_df = pd.DataFrame(
            columns=["line_item_id", "creative_name", "creative_size"]
        )
        sent_keys_list = [] 
        
        # Create key tuple for each violation
        final_df["key_tuple"] = list(zip(
            final_df["line_item_id"].astype(str),
            final_df["creative_name"].astype(str)
        ))
        
        # Check if this (line_item_id, creative_name) was already alerted
        final_df["previous_alert_status"] = final_df["key_tuple"].isin(sent_keys_list)
        
        # Create DataFrame with both columns for S3
        all_violation_keys = pd.DataFrame({
            'line_item_id': final_df['line_item_id'].astype(str),
            'creative_name': final_df['creative_name'].astype(str)
        })

        #To define for testing in place of using in aws s3 bucket function
    

        # Combine with previous keys and save
        
        # Merge previous + new keys and de-duplicate
        merged_state_df = (
        pd.concat([sent_keys_df, all_violation_keys], ignore_index=True)
    )
        wr.s3.to_csv(
            df=merged_state_df,
            path=s3_dataset_path,
            index=False
        )

        logger.info(
            "Updated S3 state for date=%s with %d total keys",
            today_date_str,
            len(merged_state_df)
        )
        
        # Filter for NEW alerts
        new_alerts_df = final_df[~final_df["previous_alert_status"]].copy(deep=True)
        
        if new_alerts_df.empty:
            logger.info("No NEW alerts (all were previously alerted)")
            return
    # Build Slack blocks for only the alerts that haven't been sent previously.
    # Group alerts by `order_trafficker` to direct messages to the right users.
        elements = []
        print(new_alerts_df.columns.tolist())
        order_group_df = new_alerts_df.groupby(["order_trafficker"])

        for i, grouped_df in order_group_df:
            # `i` is the group key (order_trafficker). Extract the email address from the stored string.
            user_email_raw: str = i[0]
            # The saved format is expected to include the email in parentheses (e.g., "Name (email)").
            # We defensively parse this and fall back to the raw string if format differs.
            try:
                user_email = user_email_raw.split("(")[1].split(")")[0]
            except Exception:
                user_email = user_email_raw
                logger.debug(
                    "Unexpected order_trafficker format; using raw value: %s",
                    user_email_raw,
                )

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
                    "text": r"The following line items require immediate attention due to a skip not enabled for creative  video duration >= 30 sec:",
                },
            },
            {"type": "divider"},
            {"type": "rich_text", "elements": elements},
        ]

        json_msg = {"blocks": blocks}

        # Send Slack notification and log the outcome. Avoid logging the webhook URL itself.
        logger.info(
            "Sending Slack notification. Result: %s, report_id=%s",
            google_ads_report_id,
        )
        # Send Slack notification via incoming webhook. We intentionally avoid logging the webhook URL.
 

        if not final_df.empty:
            # Real violation alert
            json_msg = {"blocks": blocks}
        else:
            # No violations OR no new violations
            json_msg = {
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "No skip enable alertSize violations today."
                        }
                    }
                ]
            }


    try:
        send_result = slack_notification(slack_webhook, json_msg)
        logger.info("Slack notification sent successfully")
    except Exception as e:
        logger.exception("Failed to send Slack notification")

if __name__ == "__main__":
    import os
    import awswrangler as wr
    from slack_notification import simple_slack_notification


    status_slack_webhook = get_env("STATUS_SLACK_WEBHOOK")

    simple_slack_notification(
        status_slack_webhook,
        "Skip_enabled-errors-alert Started!",
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
                f"video-enabled-error-check-{now.strftime('%Y-%m-%d_%H-%M-%S')}.log"
            )

            local_log_file = "video-skip-enabled-error-check.log"
            wr.s3.upload(local_log_file, log_key)
            print(f"✅ Log uploaded to {log_key}")
            print(f"✅ Log uploaded to {log_key}")
            simple_slack_notification(
                status_slack_webhook,
                f"Skip-not-enabled-alert completed!\n✅ Log uploaded to {log_key}",
            )
        except Exception as upload_err:
            # if upload fails, at least print to stdout
            simple_slack_notification(
                status_slack_webhook, f"⚠️ Failed to upload log to S3: {upload_err}"
            )
