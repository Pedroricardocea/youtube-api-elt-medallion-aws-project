# Analytics Views (Athena & QuickSight)

## Athena
- Workgroup: `<your-workgroup>`
- Database(s): `youtube_api_cleaned`, `dl_analytics`
- Example query ![results](./results/athena_query.csv):
```sql
SELECT region, ts , snippet_title, COUNT(*) AS videos, SUM(view_count) AS views
FROM youtube_api_analytics.final_analytics
GROUP BY region, ts, snippet_title
ORDER BY ts DESC, views DESC
LIMIT 50;
