create table if not exists person_count_by_city (
    city String,
    cnt Int64
)
engine = MergeTree()
primary key city
order by city;
