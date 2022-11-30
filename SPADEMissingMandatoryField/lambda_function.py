import json
from collections import defaultdict
import time
import boto3

from main.util import RedshiftManager
from main.util import SESManager


# Email variables
SENDER = 'EPA SPADE Missing Mandatory Fields <nbrigmon@amazon.com>'
RECIPIENT_TEST = ['nbrigmon@amazon.com'] # 
AWS_REGION = 'us-east-1'
SUBJECT = '[EPA SPADE EKG Monitoring Report] - Resolved Bugs Missing Mandatory Fields'
BUG_DATA_SQL = 'main/queries/resolved_bugs.sql' # Resolved BUG SIMS with missing required info
ALWAYS_CC = ['nbrigmon@amazon.com','spade-tpm@amazon.com']

# Data Variables
COLUMN_HEADERS = ('Title','Assignee','Missing Label','Missing Date','Missing Effort Spent','Missing Root Cause','Missing Comments')
COLUMN_IDX = {
    'link': 0,
    'title': 1,
    'assignee': 2,
    'mgr_email': 3, 
    'missing_labels': 4,
    'missing_dates': 5,
    'missing_effort': 6,
    'missing_root_cause': 7,
    'missing_comments': 8,
    'business_days': 9
}


BODY_TEXT = ("Amazon SES Test (Python) \r\n"
             "This email is sent with Amazon SES using the "
             "AWS SDK for Python (Boto)."
            )
TEMPLATE_HTML = '''
    <html>
      <body>
        <p>Hello Team, <br><br>
        
        You are receiving this email because you have Resolved Bug Tickets that are missing one or more mandatory fields. Please make updates to your corresponding bug ticket.
        This information is used to make improvements in our processes, educate our user base, and provide analysis to Senior 
        Leadership with our SIM Ticket efforts.<br><br>

        
        <div>{}</div>
        
        
        <p>Note:<br>
        Alert Cadence: Daily 8 AM Pacific Time.
        This alert examines BUG tickets resolved since Oct 1, 2022.
        If there are any questions or feedback please reach out to epa-spade@amazon.com. 
        Data for the report is pulled at 9AM, if you’ve already taken action, feel free to ignore this message.
        </p>

        
        <p>Sincerely, <br>
        Nathan Brigmon, EP&A Tech<br>
        Business Intelligence Engineer</p>
        
      </body>
    </html>
    '''
### HTML STYLES ###
border = 'border: 1px solid #99a3a4;'
background_gray = 'background-color: #dddddd;'
background_red = 'background-color: #ec7063;'
background_redd = 'background-color: red;'

def lambda_handler(event, context):
    
    def get_bug_data(reshift_conn: RedshiftManager, sql_filename) -> list:
        # Start time recording
        start_time = time.time()
        # Connect to redshift
        reshift_conn.connect()
        # Read Query
        bug_data_query = open(sql_filename, 'r').read()
        # Get list of tuple/data. Need to be mapped out into table view
        results = reshift_conn.query_db(bug_data_query)
        # Close the connection
        reshift_conn.close_connection()
        # Record the time
        print("bug_data_query done for: ", sql_filename, time.time() - start_time)
        
        return results
    
    def add_column_headers(data: list) -> str:
        style = border + background_gray
        header_html = '<tr style="{}">'.format(style)
        for col in data:
            header_html += '<td style="{}">{}</td>'.format(border, col)
        header_html += '</tr>'
        return header_html
    
    def add_row_data(data: list) -> str:
        row_html = '<tr style="{}">'.format(border)
        for idx, item in enumerate(data):
            # conditional formatting for item value, not last item and is item metric, then make red background
            style = border+background_red if item != 'No' and idx > 3 and idx < 8 else border
            
            if idx == COLUMN_IDX['link'] or idx == COLUMN_IDX['mgr_email'] or idx == COLUMN_IDX['business_days']: #if link or manager email, skip
                pass
            elif idx == COLUMN_IDX['title']:
                row_html += '<td style="{}"><a href="{}">{}</a></td>'.format(style, data[idx-1], data[idx]) ## add the link and title
            else:
                row_html += '<td style="{}">{}</td>'.format(style, data[idx])
        row_html += '</tr>'
        
        return row_html
        
    # Create the HTML format for the resolved tickets
    def html_table_formatter(data: tuple) -> str:
        html_content = ''
        # beginning of table html 
        html_content += '<table style="border-collapse: collapse; max-width:1000px;">'
    

        print(len(data))
        for idx, sim_ticket in enumerate(data):
            # first section is the header
            if idx == 0:
                html_content += add_column_headers(COLUMN_HEADERS)
            html_content += add_row_data(sim_ticket)
            
        html_content += '</table>'
        return html_content
        
    # Connect to Redshift and make three queries for each part of email
    
    ### MAIN CODE ###
    redshift_manager = RedshiftManager(AWS_REGION)
    email_manager = SESManager(SENDER, AWS_REGION)
    
    print("starting queries...")
    # Execute the queries and fetch data
    res_resolved_tickets = get_bug_data(redshift_manager, BUG_DATA_SQL)
    print("queries completed...")

    # Create list of receivers
    if len(res_resolved_tickets) != 0:
        
        # For every assignee on the list, this will put their email
        RECEIVER_DATA_PT1 = [e[ COLUMN_IDX['assignee'] ]+"@amazon.com" for e in res_resolved_tickets if e[ COLUMN_IDX['assignee'] ] is not None] # pull third column from query which is assignee
        # Create list of CC'd receivers
        CC_RECEIVER_DATA_PT1 = [e[ COLUMN_IDX['mgr_email'] ] for e in res_resolved_tickets if e[ COLUMN_IDX['mgr_email'] ] is not None] # pull fourth column from query which is manager email of assignee
    
        # Consolidating lists into a singular unique list for email and cc-email
        RECEIVER_DATA_UNIQUE = list(dict.fromkeys(RECEIVER_DATA_PT1))
        CC_RECEIVER_DATA_UNIQUE = list(dict.fromkeys(CC_RECEIVER_DATA_PT1+ALWAYS_CC)) #always CC list added
        
        # CREATE HTML SECTIONS
        table_content = html_table_formatter(res_resolved_tickets)
        #print(table_content)
        BODY_HTML = TEMPLATE_HTML.format(table_content)
        
        
        # Connect to SES and send email
        email_manager.set_recipient(RECIPIENT_TEST)
        email_manager.set_cc_recipient(RECIPIENT_TEST)
        email_manager.set_email_content(SUBJECT, BODY_TEXT, BODY_HTML)
        email_manager.send_email()
    else:
        # Connect to SES and send email
        email_manager.set_recipient(RECIPIENT_TEST)
        email_manager.set_email_content(SUBJECT, BODY_TEXT, "No data results for query, confirm with Lambda Function\nhttps://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/SPADEMissingMandatoryField?tab=code")
        email_manager.send_email()
        
    print("EMAIL SENT")
    return {
        'statusCode': 200,
        'body': json.dumps('Email sent! Success')
    }

