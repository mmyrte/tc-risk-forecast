"""Auxiliary functions for generating and processing wind fields from
TCForecast objects. Very much a work in progress.

auth: jhartman
date: 2020-09-06

"""

from io import StringIO

import geopandas as gpd
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.sql import SQL, Identifier
import h3.api.numpy_int as h3

from climada.hazard.centroids import Centroids
# from climada.hazard.tc_tracks_forecast import TCForecast
from climada.hazard.trop_cyclone import TropCyclone


INTENSITY_SERIES_TYPE = 1
""" hardcoded for now, means intensity; 2 for TC impact etc."""

CENT_QUERY = """
select 
    idx as centroid_id,
    centroid as geom, 
    dist_coast
from 
    centroids_t 
where 
    idx = any(%s::h3index[]);
"""

DSN = 'dbname=tcrisk port=57701 host=localhost'

META_TABLE = 'fcast_storms_t'
META_PTS_TABLE = 'fcast_storms_pts_t'
META_ID_SEQ = 'fcast_storms_t_id_seq'
SERIES_STAGING = 'fcast_series_staging_t'

H3_LEVEL = 6
"""cannot be changed without adapting the h3cents_t table"""

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

    cols = list(df.columns)
    if index:
        cols.insert(0, df.index.name)

    # Copy the string buffer to the database, as if it were an actual file
    with con.cursor() as c:
        c.copy_from(sio, table_name, columns=cols, sep=',')

    if autocommit:
        con.commit()


def tracks_to_db(tracks, con, dry_run=False):
    """Convert a TCForecast instance into two DFs, fetch unique ID from
    con:tblname and lock tblname until the df is written to con.
    """
    gdf_points_joint = tracks.to_geodataframe(as_points=True)

    # setup metadata dataframe
    df_meta = gdf_points_joint.drop(['geometry', 'time'], axis=1)
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

    # setup points table
    gdf_points = gdf_points_joint[['time', 'geometry']]
    gdf_points.rename_geometry('geom', inplace=True)
    gdf_points.index.name = 'id'

    with con.cursor() as curs:
        try:
            curs.execute(SQL('lock table {} in access exclusive mode;')
                         .format(Identifier(META_TABLE)))
            curs.execute(SQL('select last_value from {};')
                         .format(Identifier(META_ID_SEQ)))
            last_value = curs.fetchone()[0]
            df_meta.index += last_value+1
            gdf_points.index += last_value+1

            if not dry_run:
                df_to_postgres(df_meta, con, META_TABLE,
                               index=True, autocommit=False)
                df_to_postgres(gdf_points, con, META_PTS_TABLE,
                               index=True, autocommit=False)

            con.commit()

        except psycopg2.Error as err:
            print(err)
            con.rollback()

    return df_meta, gdf_points


def windfields_to_db(tc_hazard, tracks, storm_meta, con, dry_run=False):
    """Convert one windfield hazard generated using

    >>> TropCyclone().set_from_tracks(tracks, centroids, store_windields=True)

    to a single dataframe intensity_t; commit to db staging table.
    """
    intensity_dfs = []
    ncents = tc_hazard.centroids.size

    parallel_it = zip(storm_meta.index, tracks.data, tc_hazard.windfields)

    for (index, track, windfield) in parallel_it:
        nsteps = windfield.shape[0]

        centroid_id = np.tile(tc_hazard.centroids.centroid_id, nsteps)

        intensity_3d = windfield.toarray().reshape(nsteps, ncents, 2)
        intensity = np.linalg.norm(intensity_3d, axis=-1).ravel()

        timesteps = np.repeat(track.time.data, ncents)
        timesteps = timesteps.reshape((nsteps, ncents)).ravel()

        inten_tr = pd.DataFrame({
            'centroid_id': centroid_id,
            'value': intensity,
            'timestamp': timesteps,
        })

        inten_tr = inten_tr[inten_tr.value > tc_hazard.intensity_thres]

        inten_tr['storm_id'] = index
        intensity_dfs.append(inten_tr)

    intensity_t = pd.concat(intensity_dfs)
    intensity_t.centroid_id = intensity_t.centroid_id.apply(h3.h3_to_string)
    intensity_t['type_id'] = INTENSITY_SERIES_TYPE

    if not dry_run:
        df_to_postgres(intensity_t, con, SERIES_STAGING)

    return intensity_t


def process_trackset(tracks, dry_run=False):
    """Start separate process with separate postgres connection, calc windfield
    and push it to postgres."""
    con = psycopg2.connect(DSN)

    # fetch a subset of centroids
    tracks_gdf = tracks.to_geodataframe()
    # buffer of 6 deg around track should be sufficient in most cases
    tracks_buffer = tracks_gdf.geometry\
                              .buffer(distance=6, resolution=2)\
                              .unary_union
    h3indices = h3.polyfill(tracks_buffer.__geo_interface__, H3_LEVEL, True)
    h3indices = list(map(h3.h3_to_string, h3indices))

    centroids_gdf = gpd.read_postgis(CENT_QUERY, con, params=(h3indices,))
    centroids_gdf.centroid_id = centroids_gdf.centroid_id.apply(h3.string_to_h3)

    # prepare centroids
    centroids = Centroids.from_geodataframe(centroids_gdf)

    # calculate windfields
    tc_hazard = TropCyclone()
    tc_hazard.set_from_tracks(tracks, centroids, store_windfields=True)

    storm_meta, _ = tracks_to_db(tracks, con, dry_run)

    _ = windfields_to_db(tc_hazard, tracks, storm_meta, con, dry_run)

    con.close()

    sid = tracks.data[0].sid

    return 'sid {} done'.format(sid)
