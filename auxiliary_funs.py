"""Auxiliary functions for generating and processing wind fields from
TCForecast objects. Very much a work in progress.

auth: jhartman
date: 2020-09-06

"""

import geopandas as gpd
from io import StringIO
import numpy as np
import pandas as pd
import psycopg2

from climada.hazard.centroids import Centroids
from climada.hazard.tc_tracks_forecast import TCForecast
from climada.hazard.trop_cyclone import TropCyclone


INTENSITY_SERIES_TYPE = 1
""" hardcoded for now, means intensity; 2 for TC impact etc."""

CENT_QUERY = """
select * from centroids_joint_v where
  st_intersects(
    st_makeenvelope(%s, %s, %s, %s, 4326), geom
  )
"""

DB_NAME = 'tcrisk'

def df_to_postgres(df, con, table_name):
    """Copy a pandas Dataframe to a Postgres table using psycopg2 cursor
    copy_from feature, which apparently works best for bulk inserts. From:

    https://gist.github.com/ellisvalentiner/63b083180afe54f17f16843dd51f4394

    Parameters:
        df (pandas.DataFrame)
        con (psycopg2.extensions.connection)
        table (str)
    """
    # Write the DataFrame as csv to a buffer
    sio = StringIO()
    sio.write(df.to_csv(index=None, header=None))
    sio.seek(0)

    # Copy the string buffer to the database, as if it were an actual file
    with con.cursor() as c:
        c.copy_from(sio, table_name, columns=df.columns, sep=',')
        con.commit()


def tracks_to_storm_ids(tracks, con, tblname, write_to_db=True):
    """Convert a TCForecast instance into a DF, fetch latest ID from
    con:tblname and lock tblname until the df is written to con.
    """
    df = pd.DataFrame({
        'basetime': [tr.forecast_time for tr in tracks.data],
        'storm_id': [tr.sid for tr in tracks.data],
        'storm_name': [tr.name for tr in tracks.data],
        'ensemble_no': [tr.ensemble_number for tr in tracks.data],
        'is_ensemble': [tr.is_ensemble for tr in tracks.data],
        'basin': [tr.basin for tr in tracks.data],
        'category': [tr.category for tr in tracks.data],
    })

    with con.cursor() as curs:
        try:
            curs.execute('lock table fcast_storms_t in access exclusive mode;')
            curs.execute('select last_value from fcast_storms_t_id_seq;')
            last_value = curs.fetchone()[0]
            df.index += last_value+1

            if write_to_db:
                sio = StringIO()
                sio.write(df.to_csv(index=False, header=None))
                sio.seek(0)
                curs.copy_from(sio, tblname, columns=df.columns, sep=',')

            con.commit()

        except psycopg2.Error as e:
            print(e)
            con.rollback()

    return df


def windfields_to_intensity_df(hazard, tracks, storm_ids):
    """Convert one windfield hazard generated using

    >>> TropCyclone().set_from_tracks(tracks, centroids, store_windields=True)

    to a tuple of dataframes: storms_t and intensity_t
    """
    intensity_dfs = []
    ncents = hazard.centroids.size

    parallel_it = zip(storm_ids.index,  tracks.data, hazard.windfields)

    # for (index, track, windfield) in tqdm.tqdm(
    #   parallel_it, desc='ens member'):
    for (index, track, windfield) in parallel_it:
        nsteps = windfield.shape[0]

        centroid_id = np.tile(hazard.centroids.centroid_id, nsteps)

        intensity_3d = windfield.toarray().reshape(nsteps, ncents, 2)
        intensity = np.linalg.norm(intensity_3d, axis=-1).ravel()

        timesteps = np.repeat(track.time.data, ncents)
        timesteps = timesteps.reshape((nsteps, ncents)).ravel()

        inten_tr = pd.DataFrame({
            'centroid_id': centroid_id,
            'value': intensity,
            'timestamp': timesteps,
        })

        inten_tr = inten_tr[inten_tr.value > hazard.intensity_thres]

        inten_tr['storm_id'] = index
        intensity_dfs.append(inten_tr)

    intensity_t = pd.concat(intensity_dfs)
    intensity_t['type_id'] = INTENSITY_SERIES_TYPE

    return intensity_t


def process_one_storm(storm_tracks, write_to_db=True):
    """Start separate process with separate postgres connection, calc windfield
    and push it to postgres."""
    con = psycopg2.connect(dbname=DB_NAME)

    # subset
    storm_bounds = storm_tracks.get_bounds(1)

    # fetch centroids
    cent_gdf = gpd.read_postgis(CENT_QUERY, con, params=storm_bounds)
    cent_gdf.region_id = cent_gdf.region_id.astype('float')

    # prepare centroids
    storm_centroids = Centroids.from_geodataframe(
        cent_gdf[['geom', 'region_id', 'dist_coast', 'centroid_id']]
    )

    # calculate windfields
    storm_windfield = TropCyclone()
    storm_windfield.set_from_tracks(
        storm_tracks, storm_centroids, store_windfields=True
    )

    storm_ids = tracks_to_storm_ids(
            storm_tracks, con, 'fcast_storms_t', write_to_db
    )
    inten_df = windfields_to_intensity_df(
            storm_windfield, storm_tracks, storm_ids
    )

    if write_to_db:
        df_to_postgres(inten_df, con, 'fcast_series_staging_t')

    con.close()

    sid = storm_tracks.data[0].sid

    return 'sid {} done'.format(sid)
