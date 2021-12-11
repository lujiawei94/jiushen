from apscheduler.schedulers.background import BlockingScheduler

from script.appservice.feature.timer_task import adapter

scheduler = BlockingScheduler()


def add_jobs_to_schedule():

    scheduler.add_job(adapter.recommend_match_job, "interval", seconds=1, coalesce=True)
    scheduler.add_job(adapter.analysis_job, "interval", hours=1)


if __name__ == '__main__':
    add_jobs_to_schedule()
    scheduler.start()
