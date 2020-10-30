"""Auxiliary functions for generating and processing wind fields from
TCForecast objects. Very much a work in progress.

auth: jhartman
date: 2020-10-30

"""

from io import StringIO

import geopandas as gpd
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.sql import SQL, Identifier
import h3.api.numpy_int as h3
from shapely.geometry.polygon import Polygon

from climada.hazard import Centroids, TropCyclone

INTENSITY_SERIES_TYPE = 1
""" hardcoded for now, means intensity; 2 for TC impact etc."""

CENT_QUERY = """
select 
    idx as centroid_id,
    geom, 
    dist_coast
from 
    centroids_t 
where 
    idx = any(%s::h3index[]);
"""

DSN = 'dbname=tcrisk port=57701 host=localhost'

META_TABLE = 'fcast_storms_t'
META_PTS_TABLE = 'fcast_storms_pts_t'
SERIES_STAGING = 'fcast_series_staging_t'

H3_LEVEL = 6
"""cannot be changed without adapting the h3cents_t table"""

TRACK_BUFFER = 6
"""buffer distance around tracks to select centroids; 
in the same unit as the CRS, so 6 degrees for WGS84"""

def process_trackset(tracks, dry_run=False):
    """Start separate process with separate postgres connection, calc windfield
    and push it to postgres."""
    con = psycopg2.connect(DSN)

    # fetch a subset of centroids
    centroids = _fetch_centroids(tracks, con)

    # calculate windfields
    tc_hazard = TropCyclone()
    tc_hazard.set_from_tracks(tracks, centroids, store_windfields=True)

    storm_meta, _ = tracks_to_db(tracks, con, dry_run)

    _ = windfields_to_db(tc_hazard, tracks, storm_meta, con, dry_run)

    con.close()

    sid = tracks.data[0].sid

    return 'sid {} done'.format(sid)

def _fetch_centroids(tracks, con):
    """
    Fetches h3 centroids with their dist_coast data (and possibly exposure) from
    the database.
    
    Parameters
    ----------
    tracks: TCTracks
    con: psycopg2.connection
    
    Returns
    -------
    Centroids
        with centroid_id set to h3index hex string representation
    """
    gdf = tracks.to_geodataframe()
    buffer = gdf.geometry\
                .buffer(distance=TRACK_BUFFER, resolution=2)\
                .unary_union
    
    # in case the buffer is a multipolygon, needs polyfill per polygon
    if isinstance(buffer, Polygon):
        buffer = [buffer]
        
    h3indices = np.concatenate([
        h3.polyfill(poly.__geo_interface__, H3_LEVEL, True) 
        for poly in list(buffer)    
    ])

    h3indices = list(map(h3.h3_to_string, h3indices))

    centroids_gdf = gpd.read_postgis(CENT_QUERY, con, params=(h3indices,))
    centroids_gdf.centroid_id = centroids_gdf.centroid_id.apply(h3.string_to_h3)

    return Centroids.from_geodataframe(centroids_gdf)


def tracks_to_db(tracks, con, dry_run=False):
    """
    Convert a TCTracks instance into two DFs, write to DB, return DFs that 
    match index sequence in DB.
    """
    gdf_long = tracks.to_geodataframe(as_points=True)

    # setup metadata dataframe
    df_meta = _long_gdf_to_meta(gdf_long)

    # setup points table
    gdf_points = _long_gdf_to_pts(gdf_long)
    
    if dry_run:
        return df_meta, gdf_points

    with con.cursor() as curs:
        try:
            # lock table
            lock_query = 'lock table {} in access exclusive mode;'
            lock_query = SQL(lock_query).format(Identifier(META_TABLE))
            curs.execute(lock_query)
            
            # write to db without index; insert incerements sequence
            df_to_postgres(df_meta, con, META_TABLE, autocommit=False)
            
            # fetch sequence currval after insert to compute offset
            seq_query = "select currval(pg_get_serial_sequence(%s, 'id'));"
            curs.execute(seq_query, (META_TABLE,))
            currval = curs.fetchone()[0]
            idx_offset = currval - df_meta.index.max()
            
            # update id in local DFs; needed for foreign keys on gdf_points 
            # and timeseries
            df_meta.index += idx_offset
            gdf_points.index += idx_offset

            df_to_postgres(gdf_points, con, META_PTS_TABLE, 
                           index=True, autocommit=False)

            con.commit()

        except psycopg2.Error as err:
            print(err)
            con.rollback()

    return df_meta, gdf_points


def _long_gdf_to_pts(gdf_long):
    """extract points, timestamp, rename to fit postgis table"""
    gdf_points = gdf_long[['time', 'geometry']]
    gdf_points.rename(columns={'time': 'timestamp'}, inplace=True)
    gdf_points.rename_geometry('geom', inplace=True)
    gdf_points.index.name = 'id'
    return gdf_points
    
    
def _long_gdf_to_meta(gdf_long):
    """extract metadata, adapt to postgis structure"""
    df_meta = gdf_long.drop(['time', 'geometry'], axis=1)
    df_meta = df_meta.drop_duplicates()
    df_meta = pd.DataFrame({
        # poor man's dplyr::select
        'basetime': df_meta.forecast_time,
        'storm_id': df_meta.sid,
        'storm_name': df_meta.name,
        'ensemble_no': df_meta.ensemble_number,
        'is_ensemble': df_meta.is_ensemble,
        'basin': df_meta.basin,
        'category': df_meta.category,
        })
    df_meta.index.name = 'id'
    return df_meta


def windfields_to_db(tc_hazard, tracks, storm_meta, con, dry_run=False):
    """Convert one windfield hazard generated using

    >>> TropCyclone().set_from_tracks(tracks, centroids, store_windields=True)

    to a single dataframe intensity_t; commit to db staging table.
    """
    intensity_dfs = []
    ncents = tc_hazard.centroids.size

    parallel_it = zip(storm_meta.index, tracks.data, tc_hazard.windfields)

    for (index, track, windfield) in parallel_it:
        intensity_dfs.append(_windfield_to_df(
            windfield, tc_hazard.centroids, track.time.data, index, tc_hazard.intensity_thres
        ))

    intensity_t = pd.concat(intensity_dfs)  # concat list of dfs
    intensity_t['type_id'] = INTENSITY_SERIES_TYPE

    if not dry_run:
        df_to_postgres(intensity_t, con, SERIES_STAGING)

    return intensity_t


def _windfield_to_df(windfield, centroids, timesteps, index, threshold):
    """
    Converts a sparse windfield matrix to a DataFrame; wind intensity normalised
    across x and y direction using sqrt(x^2+y^2).
    
    Parameters
    ----------
    windfield: scipy.sparse.csr.csr_matrix
    centroids: Centroids
    timesteps: np.ndarray, dtype=datetime64
    index: int
    threshold: float
        usually the same as in TropCyclone computation
    
    Returns
    -------
    pd.DataFrame
        Columns storm_id, centroid_id, value, timestamp
    """
    nsteps = windfield.shape[0]
    ncents = centroids.size

    centroid_id = np.tile(centroids.centroid_id, nsteps)

    intensity_3d = windfield.toarray().reshape(nsteps, ncents, 2)
    intensity = np.linalg.norm(intensity_3d, axis=-1).ravel()

    timesteps = np.repeat(timesteps, ncents)
    timesteps = timesteps.reshape((nsteps, ncents)).ravel()

    inten_tr = pd.DataFrame({
        'centroid_id': centroid_id,
        'value': intensity,
        'timestamp': timesteps,
    })

    inten_tr = inten_tr[inten_tr.value > threshold]

    inten_tr['storm_id'] = index

    return inten_tr



def df_to_postgres(df, con, table_name, index=False, autocommit=True):
    """Copy a pandas Dataframe to a Postgres table using psycopg2 cursor
    copy_from feature, which apparently works best for bulk inserts. Adapted
    from:

    https://gist.github.com/ellisvalentiner/63b083180afe54f17f16843dd51f4394

    Parameters:
        df (pandas.DataFrame)
        con (psycopg2.extensions.connection)
        table (str)
        index (bool): if true, include index (using df.index.name attribute)
    """
    # Write the DataFrame as csv to a buffer
    sio = StringIO()
    sio.write(df.to_csv(index=index, header=False))
    sio.seek(0)

    # add index name to list of columns if index is in csv
    cols = list(df.columns)
    if index:
        cols.insert(0, df.index.name)

    # Copy the string buffer to the database, as if it were an actual file
    with con.cursor() as c:
        c.copy_from(sio, table_name, columns=cols, sep=',')

    if autocommit:
        try:
            con.commit()
        except psycopg2.Error as err:
            print(err)
            con.rollback()
