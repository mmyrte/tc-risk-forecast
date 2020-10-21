# tc-risk-forecast

Code needed to set up a [CLIMADA](https://github.com/CLIMADA-project/climada_python) based tropical cyclone risk forecasting system.

In the course of my master's thesis at [ETHZ WCR](https://wcr.ethz.ch/) and [Red Cross/Red Crescent Climate Centre](https://www.climatecentre.org/), I aim to effectively visualize tropical cyclone risk forecasts. This tool focuses on:

- Wind based risk, following Greg Holland's 2008 [_Revised Hurricane Pressure–Wind Model_](https://doi.org/10.1175/2008MWR2395.1) in conjunction with ECMWF [forecast tracks](https://www.ecmwf.int/en/forecasts/datasets/wmo-essential#Essential_Tropical). Omitting storm surges and precipitation for the time being.
- The spatial coincidence with human settlements/people, as represented by the [High Resolution Settlement Layer](https://arxiv.org/abs/1712.05839) (available on the [Humanitarian Data Exchange](https://data.humdata.org/search?res_format=zipped%20geotiff&organization=facebook&q=hrsl&ext_page_size=190&sort=title_case_insensitive%20asc#dataset-filter-start)).
- While there are many uncertainties that could be visually adressed, this tool consciously focuses on the uncertainty that the ensemble forecast members describe.

## Toolchain

![Swimlane Flowchart](forecast-pipeline-swimlane.svg)

A short description of the components follows. It's probably good to have a rough understanding of CLIMADA and relational databases.

### CLIMADA / Python

The forecast is run from a Python script ([process_locally.py](process_locally.py), imports [auxiliary_funs.py](auxiliary_funs.py)), which fetches TC forecast tracks from ECMWF, inserts the metadata into a DB table, uses each track's extent to fetch an exposure GeoDataFrame, and then calculate a wind speed for each exposure coordinate and time step. This is then written into a time series table. 

Use at least CLIMADA v. 1.5.1, with one additional dependency besides the ones listed in `climada_env` conda environment: `psycopg2` is needed for database connectivity.

### Database Tables and Views

Since a database allows the description of data as relations instead of a sequence of operations, it makes sense to start with the central construct. The `centroids_t` table contains a base grid of the world at 150 arcsec resolution in vector format, i.e. each point is explicitly defined. Each point on land is associated with an exposure value from `litpop_t` (so named because of the LitPop asset data used) and an administrative region from `ne_10m_admin0_t`.

This data structure sounds very inefficient when compared to a simple raster format, but the unique identity that each grid coordinate has can be leveraged to efficiently store the sparse data of the time series. As mentioned above, each track identity, coordinate, and time step is saved in a table - `fcast_series_t` (actually, it's written to a staging table per storm and then inserted into the storage table, where indices are built and constraints are checked). The track identity can be joined with the metadata in `fcast_storms_t`, the coordinate with the `centroids_t`, and the `fcast_type_t` could be used to codify several hazards and/or impact functions.

The database setup and the preparation of the fundamental data is described in [db-setup.md](db-setup/db-setup.md).

### Viz Tool

The visualisation tool that ingests the data from PostGIS could be any of the common platforms - be that tailored to GIS uses as in QGIS or kepler.gl, or more generalised BI software like Tableau or Metabase. The graph above shows off QGIS, which queries the data via ODBC, or the <https://kepler.gl/demo> platform, which caches CSV or GeoJSON files locally.
