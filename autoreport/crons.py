#!/usr/bin/env python
# coding=utf-8
import unicodecsv as csv
from django_cron import CronJobBase
from django.core.mail import EmailMultiAlternatives
from django.db import connections

class UserMasterReport(CronJobBase):
    RETRY_AFTER_FAILURE_MINS = 5
    ALLOW_PARALLEL_RUNS = True
    # RUN_AT_TIMES = ['17:30']
    # schedule = Schedule(run_at_times=RUN_AT_TIMES)
    code = 'report.UserMasterReport'

    def do(self):
        cursor = connections['usermaster'].cursor()

        # get tenants to tenants.csv
        tenantfile = 'tenants.txt'
        sql = '''
            select 
                tp.name, t.domain, tp.contact, tp.mail, tp.phone, t.create_time
            from 
                tenant as t
                join tenant_profile as tp on tp.tenant_id = t.id
            order by
                t.create_time desc;
        '''
        cursor.execute(sql)
        fields = [desc[0] for desc in cursor.description]
        with open(tenantfile, 'wb') as csvfile:
            csvwriter = csv.writer(csvfile, encoding='utf-8')
            csvwriter.writerow(fields)
            while True:
                row = cursor.fetchone()
                if not row:
                    break
                csvwriter.writerow(row)

        # get users to users.csv
        userfile = 'users.txt'
        sql = '''
            SELECT 
                t.account_id, t.username, t.domain,
                MAX(IF(t.display_name = '电话',t.`value`,NULL)) 'phone',
                MAX(IF(t.display_name = '邮箱',t.`value`,NULL)) 'email',
                MAX(IF(t.display_name = '手机',t.`value`,NULL)) 'mobile',
                t.ctime
            FROM (
                SELECT 
                    p.*,f.code,f.display_name,a.username,tt.domain, a.create_time as ctime
                FROM 
                    account_profile p 
                    LEFT JOIN profile_field f ON p.field_id = f.id 
                    LEFT JOIN account a ON p.account_id = a.id
                    left join tenant tt on a.tenant_id = tt.id
                ) t
            GROUP BY 
                t.ctime desc;
        '''
        cursor.execute(sql)
        fields = [ desc[0] for desc in cursor.description]
        with open(userfile, 'wb') as csvfile:
            spamwriter = csv.writer(csvfile, encoding='utf-8')
            spamwriter.writerow(fields)
            while True:
                row = cursor.fetchone()
                if not row:
                    break
                spamwriter.writerow(row)

        # send mails
        from_email = 'dayan.li@datayes.com'
        title = '截止到发送邮件时，用户和租户统计'
        content = '''
Hi all:

截止到发送邮件时，云平台上所有的租户和用户信息
    所有租户如附件%s，
    所有用户如附件%s。

请打开Excel导入文本数据（数据-自文本-逗号分隔）

此邮件为系统自动发送，
如果需要调整请联系 dayan.li@datayes.com
        ''' % (tenantfile,userfile)
        to_email = [
            'dayan.li@datayes.com',
            'kecheng.lu@datayes.com',
            'li.dai@qq.com',
            'wei.cao@datayes.com'
        ]
        msg = EmailMultiAlternatives(title, content, from_email, to_email)
        msg.attach_file(tenantfile)
        msg.attach_file(userfile)
        msg.send(fail_silently=False)


