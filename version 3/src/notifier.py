"""
Notification module for email and telegram alerts.
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Optional
from datetime import datetime
from zoneinfo import ZoneInfo


class Notifier:
    """Handles sending notifications via email and telegram."""
    
    # Timezone for Lagos, Nigeria
    TIMEZONE = ZoneInfo("Africa/Lagos")
    
    def __init__(
        self, 
        smtp_server: str = 'smtp.gmail.com',
        smtp_port: int = 587,
        sender_email: str = None,
        sender_password: str = None,
        telegram_bot_token: str = None,
        telegram_chat_id: str = None
    ):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.sender_password = sender_password
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
    
    def get_current_time(self) -> datetime:
        """Get current time in Lagos timezone."""
        return datetime.now(self.TIMEZONE)
    
    def send_email_alert(
        self, 
        sync_results: List[Dict], 
        recipients: List[str]
    ) -> Optional[str]:
        """
        Send HTML email with sync results.
        
        Args:
            sync_results: List of sync result dictionaries
            recipients: List of email addresses
            
        Returns:
            Error message if failed, None if successful
        """
        if not sync_results or not recipients:
            return None
        
        # Count successes and failures
        success_count = sum(1 for r in sync_results if r.get('status') == 'SUCCESS')
        failed_count = len(sync_results) - success_count
        
        # Create email
        msg = MIMEMultipart("alternative")
        msg['Subject'] = f"MySQL→BigQuery Sync Report - {success_count} Success, {failed_count} Failed"
        msg['From'] = self.sender_email or "SENDEREMAIL"
        msg['To'] = ", ".join(recipients)
        
        # Build HTML content
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                h2 {{ color: #333; }}
                .summary {{ 
                    padding: 15px; 
                    background-color: #f0f0f0; 
                    border-radius: 5px;
                    margin-bottom: 20px;
                }}
                .success {{ color: #28a745; font-weight: bold; }}
                .failed {{ color: #dc3545; font-weight: bold; }}
                table {{ 
                    border-collapse: collapse; 
                    width: 100%;
                }}
                th {{ 
                    background-color: #4CAF50; 
                    color: white;
                    padding: 12px;
                    text-align: left;
                }}
                td {{ 
                    border: 1px solid #ddd;
                    padding: 10px;
                }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                .status-success {{ color: #28a745; font-weight: bold; }}
                .status-failed {{ color: #dc3545; font-weight: bold; }}
            </style>
        </head>
        <body>
            <h2>MySQL → BigQuery Sync Pipeline Report</h2>
            <div class="summary">
                <p><strong>Sync Completed:</strong> {self.get_current_time().strftime('%Y-%m-%d %H:%M:%S WAT')}</p>
                <p><span class="success">✓ Successful: {success_count}</span> | 
                   <span class="failed">✗ Failed: {failed_count}</span></p>
            </div>
            
            <table>
                <thead>
                    <tr>
                        <th>Table</th>
                        <th>Status</th>
                        <th>Rows Synced</th>
                        <th>Columns</th>
                        <th>New Columns</th>
                        <th>Sync Time</th>
                        <th>Remarks</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        for result in sync_results:
            status_class = "status-success" if result.get('status') == 'SUCCESS' else "status-failed"
            new_cols = ", ".join(result.get('new_columns', [])) or "-"
            sync_time = result.get('sync_time')
            if sync_time:
                # Ensure sync_time is timezone-aware
                if sync_time.tzinfo is None:
                    sync_time = sync_time.replace(tzinfo=self.TIMEZONE)
                sync_time_str = sync_time.strftime('%H:%M:%S WAT')
            else:
                sync_time_str = '-'
            
            html_content += f"""
                <tr>
                    <td>{result.get('table', 'N/A')}</td>
                    <td class="{status_class}">{result.get('status', 'UNKNOWN')}</td>
                    <td>{result.get('row_count', 0)}</td>
                    <td>{result.get('column_count', 0)}</td>
                    <td>{new_cols}</td>
                    <td>{sync_time_str}</td>
                    <td>{result.get('remark', '')}</td>
                </tr>
            """
        
        html_content += """
                </tbody>
            </table>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(html_content, "html"))
        
        # Send email
        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                if self.sender_email and self.sender_password:
                    server.login(self.sender_email, self.sender_password)
                server.sendmail(msg['From'], recipients, msg.as_string())
            return None
        except Exception as e:
            return f"Email alert failed: {str(e)}"
    
    def send_telegram_message(
        self, 
        message: str
    ) -> Optional[str]:
        """
        Send message via Telegram bot.
        
        Args:
            message: Message text to send
            
        Returns:
            Error message if failed, None if successful
        """
        if not self.telegram_bot_token or not self.telegram_chat_id:
            return "Telegram not configured"
        
        try:
            import requests
            
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            payload = {
                'chat_id': self.telegram_chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            
            response = requests.post(url, json=payload)
            
            if response.status_code == 200:
                return None
            else:
                return f"Telegram API error: {response.text}"
                
        except Exception as e:
            return f"Telegram message failed: {str(e)}"
  
  
    def format_sync_summary_for_telegram(
        self, 
        sync_results: List[Dict]
    ) -> str:
        """
        Format sync results for Telegram message.
        
        Args:
            sync_results: List of sync result dictionaries
            
        Returns:
            Formatted message string
        """
        success_count = sum(1 for r in sync_results if r.get('status') == 'SUCCESS')
        failed_count = len(sync_results) - success_count
        
        message = f"<b>🔄 MySQL → BigQuery Sync Report</b>\n\n"
        message += f"<b>Time:</b> {self.get_current_time().strftime('%Y-%m-%d %H:%M:%S WAT')}\n"
        message += f"<b>✅ Success:</b> {success_count}\n"
        message += f"<b>❌ Failed:</b> {failed_count}\n\n"
        
        for result in sync_results:
            status_emoji = "✅" if result.get('status') == 'SUCCESS' else "❌"
            message += f"{status_emoji} <b>{result.get('table')}</b>\n"
            message += f"   Rows: {result.get('row_count', 0)} | "
            message += f"Cols: {result.get('column_count', 0)}\n"
            
            if result.get('new_columns'):
                message += f"   New cols: {', '.join(result.get('new_columns'))}\n"
            
            if result.get('status') == 'FAILED':
                message += f"   Error: {result.get('remark', 'Unknown')}\n"
            
            message += "\n"
        
        return message

    
    def send_telegram_sync_notification(
        self, 
        sync_results: List[Dict]
    ) -> Optional[str]:
        """
        Send sync completion notification to Telegram.
        
        Args:
            sync_results: List of sync result dictionaries
            
        Returns:
            Error message if failed, None if successful
        """
        if not self.telegram_bot_token or not self.telegram_chat_id:
            return "Telegram not configured"
        
        message = self.format_sync_summary_for_telegram(sync_results)
        return self.send_telegram_message(message)
        