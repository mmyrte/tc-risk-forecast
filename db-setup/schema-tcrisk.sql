/*
  set up database structure for tc risk, point based version
  assuming postgis and h3-pg plugins have already been set up
  auth: jhartman
  date: 2020-10-15
*/

/*
  geographic structures: populate h3cents_t from a (possibly resampled)
  version of the NASA distance to coast dataset
  https://oceancolor.gsfc.nasa.gov/docs/distfromcoast/ using some
  raster-to-point conversion tool. the ne_id is a foreign key to the Natural
  Earth administrative country polygons that must be loaded beforehand
  using e.g.
  
  shp2pgsql -d ne_10m_admin_0_countries.shp ne_10m_admin0_t | psql -d tcrisk_v1
  
  download from:
  
  https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_admin_0_countries.zip
  
  And yes, that URL's supposed to look like that.
*/

/*
exposure values from litpop - sampled using R's raster::extract at the
points from centroids_t
create table public.population_t (
    id integer
  , litpop double precision
  , foreign key (id) references public.centroids_t(id)
);

create index litpop_values_idx on public.litpop_values_t using btree (id);
*/

-- storm metadata
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

create table public.fcast_storms_pts_t (
    id int8,
    "timestamp" timestamp,
    geom geometry(Point),
    foreign key (id) references public.fcast_storms_t(id)
);

create index fcast_storms_pts_t_idx on public.fcast_storms_pts_t using btree (id);
create index fcast_storms_pts_t_geoidx on public.fcast_storms_pts_t using gist (geom);

-- time series metadata
create table public.fcast_type_t (
    id smallserial
  , "type" text
  , description text
  , primary key (id)
  , check ("type" in ('hazard', 'impact'))
  , unique ("type", description)
);
-- just hard-coding this ID for now
insert into public.fcast_type_t ("type", description)
values ('hazard', 'tc wind in m/s')
;


-- time series
create table public.fcast_series_t (
    centroid_idx h3index
  , storm_id integer
  , type_id smallint
  , "value" numeric
  , "timestamp" timestamp without time zone
  , foreign key (centroid_idx) references public.centroids_t(idx)
  , foreign key (storm_id) references public.fcast_storms_t(id)
  , foreign key (type_id) references public.fcast_type_t(id)
);

/*
  actually, these indexes get recreated after each bulk insert from the
  staging table; possibly inefficient as hell, but i'm not going to learn to
  use NoSQL for my thesis (would fit a format with e.g. one file per storm and
  forecast timestamp)
*/
create index fcast_series_centroid_idx on public.fcast_series_t using btree (centroid_idx);
create index fcast_series_storm_idx on public.fcast_series_t using btree (storm_id);

create table public.fcast_series_staging_t (
    centroid_id h3index
  , storm_id integer
  , type_id smallint
  , value numeric
  , "timestamp" timestamp without time zone
);

/*
  set up views:
    litpop_exp_v: 
      what was originally queried from the forecast script; since it's not
      actually necessary to get exposures into climada, the centroids now just
      get fetched from
    centroids_joint_v: 
      ne join necessary for setting on_land parameter.
    fcast_storms_latest_v: 
      contains the latest storms; would probably need to be materialised if the
      tool is used for a longer while
    latest_intensity_v:
      all the latest intensities, non aggregated. the join order attempts to
      make the joins efficient, but may run into scaling problems there.
      materialising instead of merely caching is probably not a bad idea.
    sally_impact_v: view used to make first few sample graphs
*/
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

create or replace view public.sally_impact_v as
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