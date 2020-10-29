"""This script generates the windfield data required for my master's thesis

auth: jhartman
date: 2020-09-08
args:
    remote_dir, optional: the name of the remote directory to load the
    predictions from.
"""
import argparse
import logging
from multiprocessing import Pool

from climada.hazard.tc_tracks_forecast import TCForecast
import numpy as np
import psycopg2
import tqdm
from tc_risk_forecast import process_trackset, DSN

NUM_PROC = 20

tclogger = logging.getLogger('climada.hazard.trop_cyclone')
tclogger.setLevel(logging.WARNING)

parser = argparse.ArgumentParser()
parser.add_argument("--remote_dir", default=None)
args = parser.parse_args()

bufr_files = TCForecast.fetch_bufr_ftp(remote_dir=args.remote_dir)

fcast = TCForecast()
fcast.fetch_ecmwf(files=bufr_files)

sids = np.unique([storm.sid for storm in fcast.data])

tracks_per_sid = [
    fcast.subset({'sid': sid}) for sid in sids
]

with Pool(NUM_PROC) as pool:
    res = list(tqdm.tqdm(
        pool.imap(process_trackset, tracks_per_sid),
        total=len(tracks_per_sid)
    ))

con = psycopg2.connect(DSN)

with con.cursor() as c:
    c.execute('drop index fcast_series_centroid_idx;')
    c.execute('drop index fcast_series_storm_idx;')
    con.commit()

    c.execute('insert into fcast_series_t'
              ' select * from fcast_series_staging_t;')
    c.execute('truncate fcast_series_staging_t;')
    con.commit()

    c.execute('create index fcast_series_centroid_idx'
              ' on fcast_series_t(centroid_id int4_ops);')
    c.execute('create index fcast_series_storm_idx'
              ' on fcast_series_t(storm_id int4_ops);')
    con.commit()

con.close()
