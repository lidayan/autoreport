#!/usr/bin/env python
# coding=utf-8

from django_cron import CronJobBase, Schedule
from django.core.mail import EmailMultiAlternatives

class UserMasterReport(CronJobBase):
    RETRY_AFTER_FAILURE_MINS = 5
    ALLOW_PARALLEL_RUNS = True
    RUN_AT_TIMES = ['17:30']

    schedule = Schedule(run_at_times=RUN_AT_TIMES)
    code = 'report.UserMasterReport'

    def do(self):
        print 'running'
        self.sendMail('good-title','asdfasdfasdf',['/tmp/test.msg'])
        print 'end'

    def sendMail(self, title, content, attches):
        from_email = 'dayan.li@datayes.com'
        to_email = ['dayan.li@datayes.com']
        msg = EmailMultiAlternatives(title, content, from_email, to_email)
        for a in attches:
            msg.attach(a)
        msg.send()


