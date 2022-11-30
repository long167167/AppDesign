import boto3

from botocore.exceptions import ClientError

class SESManager():
    def __init__(self, sender, region_name):
        self.sender = sender
        self.aws_region = region_name
        self.recipient = []
        self.cc_recipient = []

        self.subject = None
        self.body_text = None
        self.body_html = None
        self.charset = 'UTF-8'
        
        self.client = boto3.client('ses', region_name=self.aws_region)
    
    def set_recipient(self, recipient):
        self.recipient = recipient
    
    def set_cc_recipient(self, cc_recipient):
        self.cc_recipient = cc_recipient

    def set_email_content(self, subject, body_text, body_html):
        self.subject = subject
        self.body_text = body_text
        self.body_html = body_html
    

    def send_email(self):
       # print(self.recipient)
        try:
            self.client.send_email(
                Destination={
                    'ToAddresses': self.recipient,
                    'CcAddresses': self.cc_recipient
                },
                Message={
                    'Body': {
                        'Html': {
                            'Charset': self.charset,
                            'Data': self.body_html,
                        },
                        'Text': {
                            'Charset': self.charset,
                            'Data': self.body_text,
                        },
                    },
                    'Subject':{
                        'Charset': self.charset,
                        'Data': self.subject,
                    },
                },
                Source=self.sender,
            )
        except ClientError as e:
            print(e.response['Error']['Message'])