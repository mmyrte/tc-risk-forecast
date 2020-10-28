/*

step 0: generate csv of all h3 indices; set res with -r flag

bash$ echo 'idx' > ~/somedir/h3cents_res6.txt
bash$ h3ToHier -r 6 >> ~/somedir/h3cents_res6.txt

*/

create temp table h3cents_tmp0 (
  idx char(15)
);

copy h3cents_tmp0 (idx)
from '~/somedir/h3cents_res6.txt'
csv header;

/*

step 1: cast to h3index, postgis point & polygon geom

not sure how i could cast to a specific geom type with CRS
in a create table as statement, so doing it in two steps

*/
create temp table h3cents_tmp1 (
  idx h3index,
  centroid geometry(Point,4326),
  boundary geometry(Polygon,4326)
);

insert into h3cents_tmp1 (idx, centroid, boundary)
select
    idx::h3index as idx
  , h3_to_geometry(idx::h3index) as centroid
  , h3_to_geo_boundary_geometry(idx::h3index) as boundary
from
  h3cents_tmp0;

/*

step 2: use raster::extract or something similar to get distance
to coast from nasa data; insert idx and dist_coast into h3cents_tmp2

step 3: join it all into a final h3cents_t

*/

create table h3cents_t as
select
  tmp1.idx,
  tmp1.centroid,
  tmp1.boundary,
  tmp2.distcoast
from
  h3cents_tmp1 tmp1,
  h3cents_tmp2 tmp2
where
  tmp1.idx = tmp2.idx::h3index;



with
idxsets as (
  select 
    ne.id, 
    h3_polyfill(ne.geom, 6) as idxs
  from ne_h3_t ne
),
idxarrays as (
  select
    id,
    array_agg(idxs)
  from idxsets
  group by id
),
idxcompact as (
  select
    id,
    h3_compact(array_agg)
  from
    idxarrays
),
idxcomparray as (
  select
    id,
    array_agg(h3_compact) as h3_array
  from
    idxcompact
  group by id
)
update ne_h3_t
set h3_array = idxcomparray.h3_array
from idxcomparray
where ne_h3_t.id = idxcomparray.id
;

select id, iso_a3, cardinality(h3_array) from ne_h3_t order by cardinality;

-- check containment with <@ commutator
select iso_a3 from ne_h3_t where '871f8a862ffffff'::h3index <@ any(h3_array);
