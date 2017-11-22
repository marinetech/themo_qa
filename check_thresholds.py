from pymongo import MongoClient
from qa_conf import *


err_flag = "x"


def init_db():
    global client; client = MongoClient()
    global db; db = client.themo

# def get_flg():
#     cursor1= db.samples.find({'err_flag': 'x'})
#     for record in cursor1:
#         print("err_flag: " + record["err_flag"])

def clean_threshholds():
    db.samples.update_many({}, {'$unset': {'err_flag': "x"}});

def validate_brometer_values():
    # filter = {'sensor_name':'barometer', {$or [ 'BAROMETER': {'$lt': thresholds["barometer_ps_min"]}, 'BAROMETER': {'$gt': thresholds["barometer_ps_max"]} ] } }
    filter = {'sensor_name':'barometer', '$or': [ {'BAROMETER': {'$lt': thresholds["barometer_ps_min"]}}, {'BAROMETER': {'$gt': thresholds["barometer_ps_max"]}} ]}
    db.samples.update_many(
       filter,
       { '$set': { 'err_flag': err_flag } }
    )

def validate_flntu_values():
    filter = {'sensor_name':'flntu', '$or': [
                {'chlorophyll_concentration': {'$lt': thresholds["flntu_ch_min"]}},
                {'chlorophyll_concentration': {'$gt': thresholds["flntu_ch_max"]}},
                {'turbidity_units': {'$gt': thresholds["flntu_tur_max"]}},
                {'turbidity_units': {'$lt': thresholds["flntu_tur_min"]}}]}
    db.samples.update_many(
       filter,
       { '$set': { 'err_flag': err_flag } }
    )

def validate_microcat_values():
    filter = {'sensor_name':'microcat', '$or': [
                {'Salinity': {'$lt': thresholds["microcat_sali_min"]}},
                {'Salinity': {'$gt': thresholds["microcat_sali_max"]}},
                {'Conductivity': {'$gt': thresholds["microcat_cond_max"]}},
                {'Conductivity': {'$lt': thresholds["microcat_cond_min"]}},
                {'Temperature': {'$gt': thresholds["microcat_temp_max"]}},
                {'Temperature': {'$lt': thresholds["microcat_temp_min"]}}]}
    db.samples.update_many(
       filter,
       { '$set': { 'err_flag': err_flag } }
    )

def validate_s9_values():
    filter = {'sensor_name':'s9', '$or': [
                {'temperature': {'$lt': thresholds["s9_temp_min"]}},
                {'temperature': {'$gt': thresholds["s9_temp_max"]}}]}
    db.samples.update_many(
       filter,
       { '$set': { 'err_flag': err_flag } }
    )

def validate_humidity_values():
    filter = {'sensor_name':'mp101a_humidity', '$or': [
                {'AvgLinearAdjVal': {'$lt': thresholds["humidity_min"]}},
                {'AvgLinearAdjVal': {'$gt': thresholds["humidity_max"]}}]}
    db.samples.update_many(
       filter,
       { '$set': { 'err_flag': err_flag } }
    )

def validate_ext_temp_values():
    filter = {'sensor_name':'mp101a_temprature', '$or': [
                {'AvgLinearAdjVal': {'$lt': thresholds["ext_temp_min"]}},
                {'AvgLinearAdjVal': {'$gt': thresholds["ext_temp_max"]}}]}
    db.samples.update_many(
       filter,
       { '$set': { 'err_flag': err_flag } }
    )

def validate_microstrain_values():
    filter = {'sensor_name':'waves', '$or': [
                {'dominant_period': {'$lt': thresholds["waves_dominant_min"]}},
                {'dominant_period': {'$gt': thresholds["waves_dominant_max"]}},
                {'significant_height': {'$lt': thresholds["waves_significant_min"]}},
                {'significant_height': {'$gt': thresholds["waves_significant_max"]}},
                {'mean_period': {'$lt': thresholds["waves_mean_min"]}},
                {'mean_period': {'$gt': thresholds["waves_mean_max"]}}]}
    db.samples.update_many(
       filter,
       { '$set': { 'err_flag': err_flag } }
    )

def validate_metpak_values():
    filter = {'sensor_name':'metpak', '$or': [
                {'temperature': {'$lt': thresholds["ext_temp_min"]}},
                {'temperature': {'$gt': thresholds["ext_temp_max"]}},
                {'humidity': {'$lt': thresholds["humidity_min"]}},
                {'humidity': {'$gt': thresholds["humidity_max"]}},
                {'dewpoint': {'$gt': thresholds["metpak_dewpoint_max"]}},
                {'dewpoint': {'$lt': thresholds["metpak_dewpoint_min"]}},
                {'wind_direction': {'$gt': thresholds["wind_direction_max"]}},
                {'wind_direction': {'$lt': thresholds["wind_direction_min"]}},
                {'wind_speed': {'$gt': thresholds["wind_speed_max"]}},
                {'wind_speed': {'$lt': thresholds["wind_speed_min"]}},
                {'pressure': {'$gt': thresholds["barometer_ps_max"]}},
                {'pressure': {'$lt': thresholds["barometer_ps_min"]}}]}
    db.samples.update_many(
       filter,
       { '$set': { 'err_flag': err_flag } }
    )

def validate_windsonic_values():
    filter = {'sensor_name':'windsonic', '$or': [
                {'magnitude_1': {'$lt': thresholds["wind_speed_min"]}},
                {'magnitude_1': {'$gt': thresholds["wind_speed_max"]}},
                {'magnitude_2': {'$lt': thresholds["wind_speed_min"]}},
                {'magnitude_2': {'$gt': thresholds["wind_speed_max"]}},
                {'gustdirection': {'$gt': thresholds["wind_direction_max"]}},
                {'gustdirection': {'$lt': thresholds["wind_direction_min"]}},
                {'winddirection': {'$gt': thresholds["wind_direction_max"]}},
                {'winddirection': {'$lt': thresholds["wind_direction_min"]}}]}
    db.samples.update_many(
       filter,
       { '$set': { 'err_flag': err_flag } }
    )

def validate_dcs_values():
    filter = {'sensor_name':'windsonic', '$or': [
                {'Direction[DegM]': {'$lt': thresholds["wind_speed_min"]}},
                {'Direction[DegM]': {'$gt': thresholds["wind_speed_max"]}},
                {'East[cm/s]': {'$lt': thresholds["wind_speed_min"]}},
                {'East[cm/s]': {'$gt': thresholds["wind_speed_max"]}},
                {'Temperature[DegC]': {'$gt': thresholds["wind_direction_max"]}},
                {'Temperature[DegC]': {'$lt': thresholds["wind_direction_min"]}},
                {'Heading[DegM]': {'$gt': thresholds["wind_direction_max"]}},
                {'Heading[DegM]': {'$gt': thresholds["wind_direction_max"]}},
                {'North[cm/s]': {'$gt': thresholds["wind_direction_max"]}},
                {'North[cm/s]': {'$gt': thresholds["wind_direction_max"]}},
                {'Abs_Speed[cm/s]': {'$gt': thresholds["wind_direction_max"]}},
                {'Abs_Speed[cm/s]': {'$gt': thresholds["wind_direction_max"]}},
                {'Abs_Tilt[Deg]': {'$gt': thresholds["wind_direction_max"]}},
                {'Abs_Tilt[Deg]': {'$gt': thresholds["wind_direction_max"]}},
                {'DCS_MEASUREMENT': {'$gt': thresholds["wind_direction_max"]}},
                {'DCS_MEASUREMENT': {'$gt': thresholds["wind_direction_max"]}},
                {'Strength[dB]': {'$gt': thresholds["wind_direction_max"]}},
                {'Strength[dB]': {'$gt': thresholds["wind_direction_max"]}},
                {'Max_Tilt[Deg]': {'$gt': thresholds["wind_direction_max"]}},
                {'Max_Tilt[Deg]': {'$gt': thresholds["wind_direction_max"]}},
                {'Std_Tilt[Deg]': {'$gt': thresholds["wind_direction_max"]}},
                {'Std_Tilt[Deg]': {'$gt': thresholds["wind_direction_max"]}},
                {'SP_Std[cm/s]': {'$gt': thresholds["wind_direction_max"]}},
                {'SP_Std[cm/s]': {'$gt': thresholds["wind_direction_max"]}},
                {'Tilt_Y[Deg]': {'$gt': thresholds["wind_direction_max"]}},
                {'Tilt_Y[Deg]': {'$gt': thresholds["wind_direction_max"]}},
                {'Tilt_X[Deg]': {'$gt': thresholds["wind_direction_max"]}},
                {'Tilt_X[Deg]': {'$lt': thresholds["wind_direction_min"]}}]}
    db.samples.update_many(
       filter,
       { '$set': { 'err_flag': err_flag } }
    )


if  __name__ == "__main__":
    init_db()
    clean_threshholds()
    # validate_brometer_values()
    # validate_flntu_values()
    # validate_microcat_values()
    # validate_s9_values()
    # validate_humidity_values()
    # validate_ext_temp_values()
    # validate_microstrain_values()
    validate_metpak_values()
