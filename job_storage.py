# -*- coding: utf-8 -*-
import datetime
from hashlib import md5

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler

import printers
import utils
from metrics import MetricCollector
from termin_api import Buro

jobstores = {
    'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
}
scheduler = BackgroundScheduler(jobstores=jobstores)


def clear_jobs():
    logger = utils.get_logger()
    logger.info("Cleaning jobs...")

    # remove jobs scheduled more than a month ago
    for job in scheduler.get_jobs():
        if 'created_at' in job.kwargs and (datetime.datetime.now() - job.kwargs['created_at']).days >= 30:
            logger.info(f"Removing job {job.kwargs['chat_id']}", extra={'user': job.kwargs['chat_id']})
            remove_subscription(job.kwargs['chat_id'], get_md5(Buro.get_buro_by_id(job.kwargs['buro']).get_name(),
                                job.kwargs['termin']), automatic=True)


def init_scheduler():
    scheduler.start()
    if not scheduler.get_job('cleanup'):
        scheduler.add_job(clear_jobs, "interval", minutes=30, id="cleanup")
    else:
        # Just to make sure interval is always correct here
        utils.get_logger().info("Rescheduling cleanup job...")
        scheduler.reschedule_job('cleanup', trigger='interval', minutes=30)


def add_subscription(update, context, interval):
    logger = utils.get_logger()
    metric_collector = MetricCollector.get_collector()

    buro = context.user_data['buro']
    termin = context.user_data['termin_type']
    deadline = context.user_data['deadline']

    chat_id = str(update.effective_chat.id)
    full_id = get_full_id(chat_id, buro, termin)
    kwargs = {'chat_id': chat_id, 'buro': buro.get_id(), 'termin': termin, 'created_at': datetime.datetime.now(),
              'deadline': deadline}
    scheduler.add_job(printers.notify_about_termins, 'interval', kwargs=kwargs, minutes=int(interval),
                      id=full_id)

    logger.info(f'[{full_id}] Subscription for {buro.get_name()}-{termin} created with interval {interval}',
                extra={'user': chat_id})
    metric_collector.log_subscription(buro=buro, appointment=termin, interval=interval, user=int(chat_id))


def remove_subscription(chat_id, md5_, automatic=False):
    full_id = get_full_id_md5(chat_id, md5_)
    if not scheduler.get_job(full_id):
        return
    scheduler.remove_job(full_id)
    if automatic:
        utils.get_logger().info(f'[{full_id}] Subscription removed since it\'s expired', extra={'user': chat_id})
        utils.get_bot().send_message(chat_id=chat_id,
                                     text='Subscription was removed since it was created more than a month ago')
    else:
        utils.get_logger().info(f'[{full_id}] Subscription removed by request', extra={'user': chat_id})
        utils.get_bot().send_message(chat_id=chat_id, text='You were unsubscribed successfully')


def get_jobs():
    return scheduler.get_jobs()


def get_jobs_prec(chat_id):
    jobs_list = scheduler.get_jobs()
    jobs_list = [i for i in jobs_list if i.id.startswith(chat_id)]
    return jobs_list


def get_job(chat_id, buro, termin):
    full_id = get_full_id(chat_id, buro, termin)
    return scheduler.get_job(full_id)


def get_md5(buro, termin):
    return md5(f"{buro}|||{termin}".encode('utf-8')).hexdigest()


def get_full_id(chat_id, buro, termin):
    return f"{chat_id}|||{get_md5(buro, termin)}"


def get_full_id_md5(chat_id, md5_):
    return f"{chat_id}|||{md5_}"
