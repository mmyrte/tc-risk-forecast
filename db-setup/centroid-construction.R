#' auth: jhartman
#' date: 1. sept 2020
#' purpose no1: carry out one-on-one spatial join between natural earth and 150as points
#' purpose no2: extract raster values from resampled (summed) 150as litpop data

con <- DBI::dbConnect(RPostgres::Postgres(), dbname = 'tcrisk')

ne <- sf::st_read(con,query = 'select * from ne_10m_admin0_t')

# one-on-one join - probably shouldn't do it this way; see README.md

for (id in ne$id) {
  print(id)
  DBI::dbExecute(con, 'update centroids_t as c
    set
      ne_id = ne.id
    from
      ne_10m_admin_0_countries as ne
    where
      st_intersects(ne.geom, c.geom)
      and c.ne_id is null
      and c.dist_coast <= 0
      and ne.id = $1
    ', id)
}

# extract from raster using point features
pts <- 
  sf::st_read(con, 
              query = 'select id, st_x(geom) as lon, st_y(geom) as lat from litpop_150as;')
rast <- 
  raster::raster('~/Documents/1uwi/0masterthesis/fundamentals/world-litpop-150as.tif')

pts$litpop <- raster::extract(rast, pts[,c(2,3)])

DBI::dbWriteTable(con, 'litpop_values_t', pts[c('id', 'litpop')])

DBI::dbDisconnect(con)
