from app.ETL import *

from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

scheduler = BlockingScheduler()


@scheduler.scheduled_job(IntervalTrigger(hours=1))
def ETL_job():

    print("START -- ETL_job")

    woeid_trends = get_woeid_trends()

    wip_woeid_trends_df, national_trends, as_of = clean_woeid_trends(woeid_trends)

    fin_woeid_trends_df = finalize_woeid_trends_sql_format(as_of, wip_woeid_trends_df)

    wip_timeseries_trend_df, my_validation = get_timeseries_trends(national_trends)

    fin_timeseries_trend_df = finalize_timeseries_trend_sql_format(wip_timeseries_trend_df)

    engine = create_engine('sqlite:///app/data/SQLITE_public_database.db', echo=True)

    insert_cross_section_trends(engine, fin_woeid_trends_df)

    insert_timeseries_trends(engine, fin_timeseries_trend_df)

    print("END -- ETL_job")


if __name__ == "__main__":
    print("main")
    ETL_job()
#    scheduler.start()
