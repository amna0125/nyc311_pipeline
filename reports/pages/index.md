---
title: NYC 311 Service Requests — Overview
---

A live overview of NYC 311 service requests, built from the dbt gold marts in
Snowflake (`NYC_311_GOLD`).

```sql kpis
select
    sum(total_requests)                                              as total_requests,
    sum(closed_requests) * 1.0 / nullif(sum(total_requests), 0)      as pct_closed,
    sum(avg_resolution_hours * closed_requests)
        / nullif(sum(closed_requests), 0)                            as avg_resolution_hours
from nyc311.daily_requests
```

```sql backlog_kpi
select
    sum(open_requests)          as open_requests,
    sum(open_overdue_requests)  as open_overdue_requests
from nyc311.backlog
```

<BigValue data={kpis} value=total_requests fmt='#,##0' title="Total Requests"/>
<BigValue data={kpis} value=pct_closed fmt='pct1' title="% Closed"/>
<BigValue data={kpis} value=avg_resolution_hours fmt='#,##0.0' title="Avg Resolution (hrs)"/>
<BigValue data={backlog_kpi} value=open_overdue_requests fmt='#,##0' title="Open & Overdue"/>

## Daily request volume

```sql daily
select
    created_date_day,
    sum(total_requests) as requests
from nyc311.daily_requests
group by 1
order by 1
```

<LineChart data={daily} x=created_date_day y=requests yAxisTitle="Requests"/>

## Requests by borough

```sql by_borough
select
    borough,
    sum(total_requests) as requests
from nyc311.daily_requests
group by 1
order by 2 desc
```

<BarChart data={by_borough} x=borough y=requests swapXY=true/>

## Top 10 complaint types

```sql top_complaints
select
    complaint_type,
    sum(total_requests) as requests
from nyc311.daily_requests
where complaint_type is not null
group by 1
order by 2 desc
limit 10
```

<LineChart data={top_complaints} x=complaint_type y=requests swapXY=true/>

## Backlog by borough

```sql backlog_by_borough
select
    borough,
    sum(total_requests)               as total_requests,
    sum(open_requests)                as open_requests,
    sum(open_overdue_requests)        as open_overdue_requests,
    max(oldest_open_request_age_days) as oldest_open_days
from nyc311.backlog
group by 1
order by 2 desc
```

<DataTable data={backlog_by_borough}/>
