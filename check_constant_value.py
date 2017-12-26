from pymongo import MongoClient

# global variables
err_flag_const = "x"
default_num_of_docs = 10

# general functions
def init_db():
    global client; client = MongoClient()
    global db; db = client.themo

def evaluate(filter_exp, sort_exp, num_of_docs, fields_to_evaluate):
    print("evaluate(): -I- processing (" + str(num_of_docs) + " records) " + str(filter_exp) + " " + str(fields_to_evaluate))

    values = {}; const_flgs = {}
    for field in fields_to_evaluate:
        values[field] = ""
        const_flgs[field] = True

    count = db.samples.count(filter_exp)
    if count <= num_of_docs: #not enough docs to process
        print("evaluate(): -W- not enough docs to evaluate: " + str(sort_exp))
        return

    cursor = db.samples.find(filter_exp).sort( sort_exp ).skip(db.samples.count(filter_exp) - num_of_docs)

    for doc in cursor:
        for field in fields_to_evaluate:
            # print("val: {}, doc[field]: {}".format(value, doc[field]))
            if values[field] == "": #first value - nothing to compare to
                values[field] = doc[field]
            else:
                if values[field] != doc[field]: # 2 values that are not identical should be enough to determine "NO CONST"
                    const_flgs[field] = False

    # if one of the keys = True it means that we found a that one of the fields (fields_to_evaluate) is constant
    for key in const_flgs:
        if const_flgs[key]:
            print("evaluate(): -I- found constant field: " + key )
            cursor.rewind()
            for doc in cursor:
                doc["const_err"] = err_flag_const
                db.samples.replace_one({"_id": doc["_id"]}, doc)
            return

def get_relevant_fields(sensor_name):
    flds = db.sensors.find_one({'name' : sensor_name})["fields_to_display"]
    for fld in ['d_stamp', 't_stamp', 'threshold', 'const_err', 'Ping_Count', 'DCS_MEASUREMENT', 's9_id', 'depth', 'depth[m]']:
        try:
            flds.remove(fld)
        except:
            # print("get_relevant_fields(): -W- failed to remove " + fld + " (" + sensor_name + ")")
            continue
    return flds


#per sensor functions
def validate_brometer_values():
    filter_exp = {'sensor_name':'barometer'}
    sort_exp = [('d_stamp', 1) ,('t_stamp', 1)]
    num_of_docs = default_num_of_docs
    fields_to_evaluate = get_relevant_fields("barometer")

    evaluate(filter_exp, sort_exp, num_of_docs, fields_to_evaluate)

def validate_flntu_values():
    filter_exp = {'sensor_name':'flntu'}
    sort_exp = [('d_stamp', 1) ,('t_stamp', 1)]
    num_of_docs = default_num_of_docs
    fields_to_evaluate = get_relevant_fields("flntu")

    evaluate(filter_exp, sort_exp, num_of_docs, fields_to_evaluate)

def validate_microcat_values():
    filter_exp = {'sensor_name':'microcat'}
    sort_exp = [('d_stamp', 1) ,('t_stamp', 1)]
    num_of_docs = default_num_of_docs
    fields_to_evaluate = get_relevant_fields("microcat")

    evaluate(filter_exp, sort_exp, num_of_docs, fields_to_evaluate)

def validate_s9_values():
    fields_to_evaluate = get_relevant_fields("s9")
    s9_cursor = db.sensors.distinct('child_sensors')
    for s9_id in s9_cursor:
        for key in s9_id.keys():
            filter_exp = {'sensor_name':'s9', 's9_id' : key}
            sort_exp = [('d_stamp', 1) ,('t_stamp', 1)]
            num_of_docs = default_num_of_docs

            evaluate(filter_exp, sort_exp, num_of_docs, fields_to_evaluate)

def validate_humidity_values():
    filter_exp = {'sensor_name':'mp101a_humidity'}
    sort_exp = [('d_stamp', 1) ,('t_stamp', 1)]
    num_of_docs = default_num_of_docs
    fields_to_evaluate = get_relevant_fields("mp101a_humidity")

    evaluate(filter_exp, sort_exp, num_of_docs, fields_to_evaluate)

def validate_ext_temp_values():
    filter_exp = {'sensor_name':'mp101a_temprature'}
    sort_exp = [('d_stamp', 1) ,('t_stamp', 1)]
    num_of_docs = default_num_of_docs
    fields_to_evaluate = get_relevant_fields("mp101a_temprature")

    evaluate(filter_exp, sort_exp, num_of_docs, fields_to_evaluate)

def validate_microstrain_values():
    filter_exp = {'sensor_name':'waves'}
    sort_exp = [('d_stamp', 1) ,('t_stamp', 1)]
    num_of_docs = default_num_of_docs
    fields_to_evaluate = get_relevant_fields("waves")

    evaluate(filter_exp, sort_exp, num_of_docs, fields_to_evaluate)

def validate_metpak_values():
    filter_exp = {'sensor_name':'metpak'}
    sort_exp = [('d_stamp', 1) ,('t_stamp', 1)]
    num_of_docs = default_num_of_docs
    fields_to_evaluate = get_relevant_fields("metpak")

    evaluate(filter_exp, sort_exp, num_of_docs, fields_to_evaluate)

def validate_windsonic_values():
    filter_exp = {'sensor_name':'windsonic'}
    sort_exp = [('d_stamp', 1) ,('t_stamp', 1)]
    num_of_docs = default_num_of_docs
    fields_to_evaluate = get_relevant_fields("windsonic")

    evaluate(filter_exp, sort_exp, num_of_docs, fields_to_evaluate)

def validate_dcs_values():
    filter_exp = {'sensor_name':'dcs'}
    sort_exp = [('d_stamp', 1) ,('t_stamp', 1)]
    num_of_docs = default_num_of_docs
    fields_to_evaluate = get_relevant_fields("dcs")

    evaluate(filter_exp, sort_exp, num_of_docs, fields_to_evaluate)

def validate_adcp_values():
    fields_to_evaluate = get_relevant_fields("adcp")
    adcp_cursor = db.samples.distinct('depth[m]', { 'sensor_name': 'adcp' })
    for adcp_depth in adcp_cursor:
        print(adcp_depth)
        filter_exp = {'sensor_name':'adcp', 'depth[m]' : adcp_depth}
        sort_exp = [('d_stamp', 1) ,('t_stamp', 1)]
        num_of_docs = default_num_of_docs

        evaluate(filter_exp, sort_exp, num_of_docs, fields_to_evaluate)

# main body
if  __name__ == "__main__":
    init_db()
    validate_brometer_values()
    validate_flntu_values()
    validate_microcat_values()
    validate_s9_values()
    validate_humidity_values()
    validate_ext_temp_values()
    validate_microstrain_values()
    validate_metpak_values()
    validate_windsonic_values()
    validate_dcs_values()
    validate_adcp_values()
