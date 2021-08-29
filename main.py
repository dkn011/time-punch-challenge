import pprint

from sqlalchemy import create_engine, Table, MetaData, func
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base

import pendulum



# Create engine and reflective models
engine = create_engine('postgresql://postgres:time_punch@localhost:5432/time_punch')
Base = automap_base()
Base.prepare(engine, reflect=True)


Employee = Base.classes['employee']
Activity = Base.classes['employee_activity']

Session = sessionmaker(engine)



TIME_FORMAT = 'YYYY-MM-DD h:mm A'
TO_RETURN = []

def get_duration_in_secs(activity):
    start_time = pendulum.parse(activity['start_time'])
    end_time = pendulum.parse(activity['end_time'])
    return (end_time - start_time).total_seconds()


def format_timestamp(iso_string):
    return pendulum.parse(iso_string).in_timezone('US/Pacific').format('YYYY-MM-DD h:mm A')


def rollup(employee_name, to_rollup, remaining_activities):
    max_idx = len(remaining_activities) - 1
    for idx, activity in enumerate(remaining_activities):

        next_activity = {} if idx == max_idx else remaining_activities[idx + 1]
        next_next_activity = {} if idx + 1 >= max_idx else remaining_activities[idx + 2]

        # If the next activity is the add to merge list and recurse
        if activity['activity_name'] == next_activity.get('activity_name'):
            rollup(employee_name, to_rollup + [activity], remaining_activities[1:])
            break

        # If the next activity is different but only <= 5 min in duration, and followed by the same
        # activity as current, also rollup.
        if (next_next_activity and
            next_next_activity['activity_name'] == activity['activity_name'] and
            get_duration_in_secs(next_activity) <= 300):
            rollup(employee_name, to_rollup + [activity, next_activity, next_next_activity], remaining_activities[3:])
            break

        if to_rollup:
            to_rollup += [activity]
            TO_RETURN.append([
                    employee_name,
                    to_rollup[0]['activity_name'],
                    format_timestamp(to_rollup[0]['start_time']),
                    format_timestamp(to_rollup[-1]['end_time']),
                    to_rollup[0]['start_time']
                ])
        else:
            TO_RETURN.append([
                employee_name,
                activity['activity_name'],
                format_timestamp(activity['start_time']),
                format_timestamp(activity['end_time']),
                activity['start_time'],
            ])

        to_rollup = []

def query_with_rollup():
    session = Session()
    try:
        query = session.query(Employee).join(Activity, Activity.employee_id == Employee.id)
        query = query.with_entities(
            Employee.id,
            Employee.name,
            func.json_agg(
                aggregate_order_by(
                    func.json_build_object(
                        'employee_id', Activity.employee_id,
                        'activity_id', Activity.id,
                        'activity_name', Activity.activity_name,
                        'start_time', Activity.start_time,
                        'end_time', Activity.end_time
                    ), Activity.start_time.asc()),
                ).label('activity_agg'),
            ).order_by(Employee.id).group_by(Employee.id, Employee.name)
    
        for _, employee_name, json_agg in query.all():
            rollup(employee_name, [], json_agg)
        for item in sorted(TO_RETURN, key=lambda x: pendulum.parse(x[4])):
            print(f"{item[0]} {item[1]} {item[2]} {item[3]}")
    finally:
        session.close()

query_with_rollup()
