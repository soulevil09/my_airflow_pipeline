import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

def enviar_email(file_path, sender_email, receiver_email, password, subject):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    body = "Segue em anexo o relatório gerado pelo Spark"
    msg.attach(MIMEText(body, 'plain'))

    # Anexar PDF
    with open(file_path, "rb") as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={file_path.split("/")[-1]}')
        msg.attach(part)

    with smtplib.SMTP('smtp.mailersend.net', 587) as server:
        server.starttls()
        server.login(sender_email, password)
        server.send_message(msg)

    