#encoding=utf-8
import sys
import smtplib
import base64
import email
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import Encoders
import os
import xlwt

class EmailSender(object):
    def __init__(self, host, port, pw, user, msg_from, msg_from_show=None):
        self.host = host
        self.port = port
        self.pw = pw
        self.user = user
        self.msg_from = msg_from
        self.msg_from_show = msg_from_show if msg_from_show is not None else msg_from

    def send_html_email(self, msg_title, msg_content, msg_to, msg_cc=None, msg_bcc=None, file_names=None):
        msg_root = MIMEMultipart('related')
        msg_root['Subject'] = msg_title
        msg_root['From'] = self.msg_from_show
        msg_root['To'] = ','.join(msg_to)
        if msg_cc is not None and len(msg_cc) > 0:
            msg_root['CC'] = ','.join(msg_cc)
        else:
            msg_cc = []
        if msg_bcc is not None and len(msg_bcc) > 0:
            msg_root['BCC'] = ','.join(msg_bcc)
        else:
            msg_bcc = []
        msg_alternative = MIMEMultipart('alternative')
        msg_root.attach(msg_alternative)
        msg_text = MIMEText(msg_content.encode('utf-8'), 'html', 'utf-8')
        msg_alternative.attach(msg_text)
        if file_names is not None and len(file_names) > 0:
            for filename in file_names:
                part = MIMEBase('application', "octet-stream")
                part.set_payload(open(filename,"rb").read())
                Encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment; filename="%s"'
                                % os.path.basename(filename))
                msg_root.attach(part)

        smtp = smtplib.SMTP()
        smtp.connect(self.host, self.port)
        smtp.ehlo()
        smtp.starttls()
        smtp.login(self.user, self.pw)
        smtp.sendmail(self.msg_from, msg_to + msg_cc + msg_bcc, msg_root.as_string())
        smtp.quit()

class ExcelHolder():
    def __init__(self):
        self.wbk = xlwt.Workbook()

    #rows: [[],[]]
    def add_new_sheet(self, sheet_name, rows=None):
        f_sheet = self.wbk.add_sheet(sheet_name, cell_overwrite_ok=True)
        if rows:
            for i in range(0, len(rows)):
                row = rows[i]
                for j in range(0, len(row)):
                    f_sheet.write(i, j, row[j])

    def save_to(self, path_name):
        self.wbk.save(path_name)


class HtmlHolder():
    CSS_BLOCK = u'''
        .tg {
            border-collapse:collapse;
            border-spacing:0;
            border-width:1px;
            border-style:solid;
            word-break:normal;
            font-family:Arial, sans-serif;
            font-size:14px;
            overflow:hidden;
        }
        .tg caption {
            text-align: left;
        }
        .tg td {
            padding:10px 5px;
            border-collapse:collapse;
            border-spacing:0;
            border-width:1px;
            border-style:solid;
            word-break:normal;
            font-family:Arial, sans-serif;
            font-size:14px;
            overflow:hidden;
        }
        .tg th {
            padding:10px 5px;
            border-collapse:collapse;
            border-spacing:0;
            border-width:1px;
            border-style:solid;
            word-break:normal;
            font-family:Arial, sans-serif;
            font-size:14px;
            overflow:hidden;
        }
        .tg .tg-x-x-1 {background-color:#b3d1c1}
        .tg .tg-x-x-2 {background-color:#a79496}
        .tg .tg-x-x-3 {background-color:#dbd9b7}
        .tg .tg-x-x-4 {background-color:#f1ff2d}
        .tg .tg-x-x-5 {background-color:#e57b85}
        .tg .tg-x-x-6 {background-color:#c7ffec}
        .tg .tg-x-x-7 {background-color:#c3bed4}
        .tg .tg-x-x-8 {background-color:#00ff80}
    '''

    HTML_FILE = u'''<!DOCTYPE html>
        <html>
        <head lang="en">
            <meta charset="UTF-8">
            <style type="text/css">
                %s
            </style>
        </head>
        <body>
            %s
        </body>
        </html>
    '''

    TG_BG_CLASS = [
    'tg-x-x-1',
    'tg-x-x-2',
    'tg-x-x-3',
    'tg-x-x-4',
    'tg-x-x-5',
    'tg-x-x-6',
    'tg-x-x-7',
    'tg-x-x-8',
    ]


    def __init__(self):
        self.html_tables = []

    def get_html_content(self):
        body_contents = [u'<p>%s</p>'%it for it in self.html_tables]
        body_contents = u'<br>'.join(body_contents)
        return HtmlHolder.HTML_FILE % (HtmlHolder.CSS_BLOCK, ''.join(body_contents))


    #table: [[], []]
    #table_head: []
    def add_table(self, caption, table_head, table):
        if len(table_head) + 1 == len(table[0]):
            table_head = [''] + table_head
        head_html = self.__html_head_row__(table_head)
        data_html = '\n'.join([self.__html_data_row__(it) for it in table])
        caption_html = u'<caption>%s</caption>' % caption
        table_html = u'<table class="tg">%s%s%s</table>' % (caption_html, head_html, data_html)

        self.html_tables.append(table_html)

    def __html_data_row__(self, text_list):
        return u'<tr>%s</tr>' % '\n'.join([self.__html_cell__(it) for it in text_list])

    def __html_head_row__(self, text_list):
        l = len(HtmlHolder.TG_BG_CLASS)
        return u'<tr>%s</tr>' % '\n'.join([self.__html_cell__(it, HtmlHolder.TG_BG_CLASS[idx%l]) for idx,it in enumerate(text_list)])


    def __html_cell__(self, text, css_class=None, rowspan=None, colspan=None):
        attr_text = ''
        if css_class is not None:
            attr_text = '%s class="%s"' % (attr_text, css_class)
        if rowspan is not None:
            attr_text = '%s rowspan="%s"' % (attr_text, rowspan)
        if colspan is not None:
            attr_text = '%s colspan="%s"' % (attr_text, colspan)
        th = u'<th%s>%s</th>' % (attr_text, text)
        return th



