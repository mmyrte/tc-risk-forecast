#' auth: jhartman
#' date: 2020-10-25
#' purpose: extract raster values from resampled (summed) 150as litpop data

con <- DBI::dbConnect(RPostgres::Postgres(), dbname = 'tcrisk')

# extract from raster using point features
pts <- sf::st_read(
    con, query = 'select idx, st_x(centroid) as lon, st_y(centroid) as lat from h3cents_t;'
)
rast <- raster::raster('dist2coast-nasa-150as.tif')

pts$dist_coast <- raster::extract(rast, pts[,c(2,3)])

DBI::dbWriteTable(con, 'h3cents_tmp', pts[c('idx', 'dist_coast')])

DBI::dbDisconnect(con)

