"""
Main entry point for MySQL to BigQuery sync pipeline.
"""
from sync_orchestrator import SyncOrchestrator


def handler(request=None):
    """
    Main handler function for the sync pipeline.
    Can be triggered by Cloud Function, Cloud Run, or locally.
    
    Args:
        request: Optional request object (for Cloud Functions)
        
    Returns:
        Success message string
    """
    # Initialize orchestrator
    orchestrator = SyncOrchestrator(config_path='tables.yml')
    
    # Run sync pipeline
    sync_results = orchestrator.run_sync_pipeline()
    
    # Print summary
    success_count = sum(1 for r in sync_results if r['status'] == 'SUCCESS')
    failed_count = len(sync_results) - success_count
    
    print(f"\n{'='*60}")
    print(f"SYNC PIPELINE COMPLETED")
    print(f"{'='*60}")
    print(f"Total tables: {len(sync_results)}")
    print(f"✓ Successful: {success_count}")
    print(f"✗ Failed: {failed_count}")
    print(f"{'='*60}\n")
    
    for result in sync_results:
        status_symbol = "✓" if result['status'] == 'SUCCESS' else "✗"
        print(f"{status_symbol} {result['table']}: {result['status']} - {result['row_count']} rows")
    
    return f"Pipeline executed. {success_count} successful, {failed_count} failed."


if __name__ == "__main__":
    # Run locally
    handler()
