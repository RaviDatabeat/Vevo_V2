from gamservices import GAMReportClient
import tempfile
import pandas as pd
import os
import time

APPLICATION_NAME = "MyGAMReportScript"
NETWORK_CODE = 40576787
SERVICE_ACCOUNT_PATH = r"C:\work\vevomaintest\mediamint-service-account.json"
SAVED_QUERY_ID = 16521056117  # Your saved query ID

def download_saved_query_report():
    """Run a saved query and download the report."""
    
    print("Initializing GAM client...")
    gam = GAMReportClient.from_service_account_file(
        application_name=APPLICATION_NAME,
        network_code=NETWORK_CODE,
        service_account_path=SERVICE_ACCOUNT_PATH,
    )
    
    print(f"\nFetching saved query ID: {SAVED_QUERY_ID}")
    
    try:
        # 1. Get the saved query
        saved_query = gam.get_saved_query(SAVED_QUERY_ID)
        print(f" Found saved query: '{saved_query.name}'")
        
        # 2. Check if it has a valid reportQuery
        if not hasattr(saved_query, 'reportQuery') or saved_query.reportQuery is None:
            print(" Error: This saved query has no valid report query definition.")
            return None
        
        # 3. Create a NEW report job from the saved query
        print("\nCreating new report job from saved query...")
        job = gam.run_report(saved_query)
        report_job_id = job["id"]
        print(f" Created new report job ID: {report_job_id}")
        
        # 4. Wait for report to complete
        print("\nWaiting for report to complete...")
        status = "IN_PROGRESS"
        attempts = 0
        max_attempts = 2
        
        while status == "IN_PROGRESS" and attempts < max_attempts:
            status = gam.report_service.getReportJobStatus(report_job_id)
            attempts += 1
            print(f"  Attempt {attempts}/{max_attempts}: Status = {status}")
            
            if status == "IN_PROGRESS":
                time.sleep(30)  # Wait 30 seconds
            elif status == "COMPLETED":
                break
            elif status == "FAILED":
                print(" Report job failed.")
                return None
        
        if status != "COMPLETED":
            print(f" Report did not complete. Final status: {status}")
            return None
        
        # 5. Download the report using DownloadReportToFile
        print("\nDownloading report...")
        report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)
        
        # Get the downloader service
        report_downloader = gam.ad_manager_client.GetDataDownloader(version='v202508')
        
        # Download to file
        report_downloader.DownloadReportToFile(
            report_job_id, 'CSV_DUMP', report_file)
        
        report_file.close()
        
        print(f' Report downloaded to:\n{report_file.name}')
        print(f'File size: {os.path.getsize(report_file.name):,} bytes')
        
        return report_file.name, report_job_id
        
    except Exception as e:
        print(f" Error: {type(e).__name__}: {e}")
        return None

# Run the function
if __name__ == "__main__":
    result = download_saved_query_report()
    
    if result:
        file_path, job_id = result
        
        # Read and display the CSV
        print("\n" + "="*60)
        print("REPORT DATA PREVIEW")
        print("="*60)
        
        df = pd.read_csv(file_path, compression='gzip', low_memory=False)
        
        print(f"\n📊 Report Summary:")
        print(f"   Report Job ID: {job_id}")
        print(f"   Rows: {df.shape[0]:,}")
        print(f"   Columns: {df.shape[1]}")
        
        print(f"\n📋 First 5 rows:")
        print(df.head())
        
        print(f"\n🏷️  Column names:")
        for i, col in enumerate(df.columns, 1):
            print(f"   {i:2d}. {col}")
        
        # Save to a permanent CSV file
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"GAM_Report_{SAVED_QUERY_ID}_{timestamp}.csv"
        
        df.to_csv(csv_filename, index=False)
        print(f"\n💾 Saved as CSV: {csv_filename}")
        
        # Optional: Clean up temp file
        os.unlink(file_path)
        print(f"🧹 Cleaned up temp file: {file_path}")
        
        print("\n✅ Process completed successfully!")