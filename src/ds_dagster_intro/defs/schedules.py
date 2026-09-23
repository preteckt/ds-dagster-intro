"""Schedules for the telematics pipeline.

Cron syntax cheat sheet
-----------------------
A cron expression has five space-separated fields:

    ┌───────────── minute        (0 - 59)
    │ ┌─────────── hour          (0 - 23)
    │ │ ┌───────── day of month  (1 - 31)
    │ │ │ ┌─────── month         (1 - 12)
    │ │ │ │ ┌───── day of week   (0 - 6, Sunday = 0)
    │ │ │ │ │
    * * * * *

Special characters:
    *     any value                 "* * * * *"     every minute
    ,     list of values            "0 8,17 * * *"  08:00 and 17:00
    -     range of values           "0 9 * * 1-5"   09:00 Monday to Friday
    /     step values               "*/15 * * * *"  every 15 minutes

Common examples:
    "* * * * *"      every minute
    "*/15 * * * *"   every 15 minutes
    "0 * * * *"      every hour, on the hour
    "0 6 * * *"      every day at 06:00
    "30 9 * * 1-5"   weekdays at 09:30
    "0 0 1 * *"      midnight on the 1st of every month

Times are interpreted in ``execution_timezone`` (UTC below).
"""

import dagster as dg

# A job that materializes every asset in the "telematics" group, in DAG order.
telematics_job = dg.define_asset_job(
    name="telematics_job",
    selection=dg.AssetSelection.groups("telematics"),
)

# Initially STOPPED so it doesn't fire unexpectedly; turn it on from the
# Automation tab in the Dagster UI.
daily_telematics_schedule = dg.ScheduleDefinition(
    name="daily_telematics_schedule",
    job=telematics_job,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
