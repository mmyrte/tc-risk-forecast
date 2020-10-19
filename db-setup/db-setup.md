# DB Setup and Data Preparation

The database that I'm using is [PostgreSQL](https://www.postgresql.org/) v12 with the [PostGIS](https://postgis.net/) and [h3-pg](https://github.com/bytesandbrains/h3-pg) extensions. A nice way of getting that up and running locally on macos is using the [Postgres.app](https://postgresapp.com/), but you're not constrained regarding the platform.

The schema described below can also be found in [schema-tcrisk.sql](schema-tcrisk.sql) without the long explanations in between. Since there are dependencies that cannot be met by merely running a sql script, you should follow the instructions here first and only then run the schema in the indicated chunks.

##   Centroids (Geographic Scaffolding)

First and foremost: make sure you've installed the relevant extensions and set up a database; I'm calling mine `tcrisk_v1`. Then import the Natural Earth [administrative country multipolygons](https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_admin_0_countries.zip) using, e.g.:

```sh
shp2pgsql -d ne_10m_admin_0_countries.shp ne_10m_admin0_t | psql -d tcrisk_v1
```

As a first step, we populate the `centroids_t` from a (possibly resampled)  version of the NASA [distance to coast](https://oceancolor.gsfc.nasa.gov/docs/distfromcoast/) dataset using some raster-to-point conversion tool. I used QGIS's `Raster pixels to points`, but you could also use R's `raster::rasterToPoints` or something similar in your toolkit of choice. Rasterize the Natural Earth vectors on the `gid` field to the exact same resolution that the distance to coast data use, then convert it back using rasterToPoints and join the two tables. Insert that into the database. If you have irregularly distributed centroids, consider using the `raster::extract` method mentioned below. 

> I initially used a one-on-one spatial join in the database. This required using a sequential for loop of DB, but was still quite inefficient; the whole join ran in a couple of hours on my laptop with 16GiB RAM, NVME SSD, 2014 Core i7. Without the one-on-one constraint, I had to abort the query after 12+ hours.

```sql
create table public.centroids_t (
    id serial
  , geom public.geometry(point,4326)
  , dist_coast numeric
  , ne_id serial
  , primary key (id)
  , foreign key (ne_id) references public.ne_10m_admin0_t(gid)
);
```

Then, only _after_ you've populated the table, construct the indexes. This is more efficient than updating the index with each chunk insert.

```sql
create index centroids_t_geom_idx on public.centroids_t using gist (geom);
create index centroids_t_idx on public.centroids_t using btree (id);
```

## Exposures

To associate the exposures with the centroids, I sampled from a rasterized version of the [LitPop](https://www.research-collection.ethz.ch/handle/20.500.11850/331316) dataset using R's `raster::extract` using the points from `centroids_t`.

```sql
create table public.litpop_values_t (
    id integer
  , litpop double precision
  , foreign key (id) references public.centroids_t(id)
);

create index litpop_values_idx on public.litpop_values_t using btree (id);
```

## Time Series Metadata

The forecast time series that we'll be generating will have three references: (a) geographic features (b) storm metadata, and (c) time series type. We already have (a) in place, now let's create (b):

```sql
create table public.fcast_storms_t (
    id serial
  , basetime timestamp without time zone
  , storm_id character(3) not null
  , storm_name text
  , ensemble_no smallint
  , is_ensemble boolean
  , basin text
  , category text
  , insert_time timestamp without time zone default now()
  , primary key (id)
);

create index fcast_storms_idx on public.centroids_t using btree (id);
```

Except for the `insert_time` and the primary key, which are set upon insertion into the table, the metadata are contained in the BUFR files from ECMWF.

The third reference in the time series identifies the series type; this can be used to register unique impact functions and hazard types. Currently, it only contains one hard-coded ID for TC wind speed as a hazard.

```sql
create table public.fcast_type_t (
    id smallserial
  , "type" text
  , description text
  , primary key (id)
  , check ("type" in ('hazard', 'impact'))
  , unique ("type", description)
);
insert into public.fcast_type_t ("type", description)
values ('hazard', 'tc wind in m/s')
;
```

## Time series

Now, it's time to set up the time series table. This will grow very large if the tool is used operationally for some time and will quite surely require tuning by an actual DBA. This may even be a reason to abandon this entire relational database - a NoSQL database could avoid a lot of the indexing overhead that is necessary here. I haven't ever worked with such a DB, so please be aware that this is developed as part of a master's thesis by an environmental scientist.

```sql
-- time series
create table public.fcast_series_t (
    centroid_id integer
  , storm_id integer
  , type_id smallint
  , "value" numeric
  , "timestamp" timestamp without time zone
  , foreign key (centroid_id) references public.centroids_t(id)
  , foreign key (storm_id) references public.fcast_storms_t(id)
  , foreign key (type_id) references public.fcast_type_t(id)
);
```

The indexes get recreated after each bulk insert from the staging table into the main table; currently, this is more efficient than appending to the indexes, even if the build is deferred to the end of the transaction.

```sql
create index fcast_series_centroid_idx on public.fcast_series_t using btree (centroid_id);
create index fcast_series_storm_idx on public.fcast_series_t using btree (storm_id);

create table public.fcast_series_staging_t (
    centroid_id integer
  , storm_id integer
  , type_id smallint
  , value numeric
  , "timestamp" timestamp without time zone
);
```

## Views

###  litpop_exp_v / centroids_joint_v

This was originally queried from the forecast script; since it's not currently necessary to get exposures into climada, the centroids now just get fetched from `centroids_joint_v`.

```sql
create view public.litpop_exp_v as
select
    c.id as centroid_id
  , public.st_y(c.geom) as latitude
  , public.st_x(c.geom) as longitude
  , li.litpop as value
  , c.dist_coast
  , c.geom as geometry
  , (ne.iso_n3)::integer as region_id
from 
  public.centroids_t c
left join 
  public.litpop_values_t li on (c.id = li.id)
left join 
  public.ne_10m_admin0_t ne on (c.ne_id = ne.gid);

create view public.centroids_joint_v as
select
    c.id as centroid_id
  , c.geom,
  , c.dist_coast
  , n.iso_n3 as region_id
from 
  centroids_t c
left join 
  ne_10m_admin0_t n on (c.ne_id = n.id);
```

### fcast_storms_latest_v

Contains the latest storms/forecast runs; this would probably need to be materialised or made into an auxiliary table if the tool is used for a while.

```sql
create or replace view public.fcast_storms_latest_v as
with latest as (
  select max(basetime) as max
  from public.fcast_storms_t
)
select 
    fcast_storms_t.*
  , latest.max
from 
  public.fcast_storms_t
inner join
   latest on (fcast_storms_t.basetime = latest.max);
```

### latest_intensity_v

This view contains all the latest intensities in a non-aggregated form. The join order is intended to be efficient, but may scale badly. Materialising instead of simply relying on caching may be necessary, since the MV would only need to be populated twice a day.

```sql
create or replace view public.latest_intensity_v as
select 
    ce.geom
  , ne.iso_a3
  , ser."timestamp"
  , ser.value as intensity
  , li.litpop as exposure
  , stz.basetime
  , stz.storm_name
  , stz.category
  , stz.basin
  , stz.is_ensemble
  , stz.ensemble_no
from 
  public.fcast_storms_latest_v stz
join 
  public.fcast_series_t ser on (stz.id = ser.storm_id)
join 
  public.centroids_t ce on (ser.centroid_id = ce.id)
left join 
  public.litpop_values_t li on (ce.id = li.id)
left join 
  public.ne_10m_admin0_t ne on (ce.ne_id = ne.gid);
```

### sally_impact_v

This example view was used to make first few sample graphs. This was only chosen because the particular forecast run for Sally at 2020-09-14, 00 UTC was shortly before landfall and was well-particularly well-suited to show off the evolution of the storm. The `group by` clause on line 22-23 means that the `avg()` operators are applied for each unique timestamp/centroid combination. The `h3_geo_to_h3` operator naively maps centroids to H3 indices; really, the spatial aggregation should happen on the h3 indices to avoid overplotting. However, this was really just a messy trial run and is documented for later improvement.

```sql
create view public.sally_impact_v as
with sally_ids as (
  select id from public.fcast_storms_latest_v
  where 
    storm_name = 'sally'
    and is_ensemble 
    and fcast_storms_latest_v.ensemble_no <= 50
), 
inten_agg as (
  select 
      s.centroid_id
    , avg(s.value) as intensity
    , (count(s.*) / 50) as density
    , avg(l.litpop) as exposure
    , s."timestamp"
  from 
    sally_ids
  join 
    public.fcast_series_t s on (sally_ids.id = s.storm_id)
  join 
    public.litpop_values_t l on (s.centroid_id = l.id)
  group by 
    s.centroid_id, s."timestamp"
)
select 
    inten_agg.intensity
  , inten_agg.density
  , inten_agg.exposure
    -- the date_part is used to cast to unix epoch in seconds
  , date_part('epoch', inten_agg."timestamp") as "timestamp"
  , h3_geo_to_h3(c.geom, 6) as h3_6
from 
  inten_agg
join 
  public.centroids_t c on (inten_agg.centroid_id = c.id)
;
```
