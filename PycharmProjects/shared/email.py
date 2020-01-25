
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
import smtplib


def send_email_with_attachment(**kwargs):

    if 'send_from' in kwargs.keys():
        send_from = kwargs['send_from']
    else:
        send_from = 'kocatulum@gmail.com'

    if 'send_to' in kwargs.keys():
        send_to = kwargs['send_to']
    else:
        send_to = 'kocatulum@gmail.com'

    if 'username' in kwargs.keys():
        username = kwargs['username']
    else:
        username = 'kocatulum'

    if 'password' in kwargs.keys():
        password = kwargs['password']
    else:
        password = 'H5Vsh7S2vmyz'

    if 'email_text' in kwargs.keys():
        email_text = kwargs['email_text']
    else:
        email_text = ''

    if 'sender_account_alias' in kwargs.keys():
        sender_account_alias = kwargs['sender_account_alias']
    else:
        sender_account_alias = 'kocatulum@gmail.com'

    if 'attachment_list' in kwargs.keys():
        attachment_list = kwargs['attachment_list']
    else:
        attachment_list = []

    if 'attachment_name_list' in kwargs.keys():
        attachment_name_list = kwargs['attachment_name_list']
    else:
        attachment_name_list = []

    subject = kwargs['subject']

    msg = MIMEMultipart()
    msg['From'] = send_from

    if isinstance(send_to,list):
        msg['To'] = ','.join(send_to)
    else:
        msg['To'] = send_to

    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    if email_text != '':
        msg.attach(MIMEText(email_text))

    if len(attachment_list)>=0:
        for i in range(len(attachment_list)):
                part = MIMEBase('application', "octet-stream")
                part.set_payload(open(attachment_list[i], "rb").read())
                encoders.encode_base64(part)
                if len(attachment_name_list)!=len(attachment_list):
                    part.add_header('Content-Disposition', 'attachment; filename=file-' + str(i+1) + '.xlsx' + '')
                else:
                    part.add_header('Content-Disposition', 'attachment; filename=' + attachment_name_list[i])
                msg.attach(part)

    if sender_account_alias=='kocatulum@gmail.com':
        server = smtplib.SMTP('smtp.gmail.com:587')
    elif sender_account_alias=='wh_trading':
        server = smtplib.SMTP('10.3.0.253',25)

    server.ehlo()

    if sender_account_alias=='kocatulum@gmail.com':
        server.starttls()
        server.login(username,password)

    server.sendmail(send_from, send_to, msg.as_string())
    server.quit()



