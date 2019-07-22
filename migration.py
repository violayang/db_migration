import os
import subprocess
import sys
import json


def runcmd (cmd):
    try:
        expcode = subprocess.run(cmd, shell=True)
        return expcode
    except OSError as e:
        return "Failed: " + str(e)



def connect_string(ocid):

    #db_ocid = input('Enter Database ocid: ')
    oci_get_cmd = "oci db database get --database-id " + ocid

    db_get = subprocess.run(oci_get_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    db_get_output = json.loads(db_get.stdout.decode("utf-8"))
    db_get_error = str(db_get.stderr.decode("utf-8"))

    if db_get.returncode == 0:
        connect_string = db_get_output["data"]["connection-strings"]["cdb-default"]
        return connect_string
    else:
        return db_get_error



def db_export():

    db_ocid = input('\nEnter Database ocid: ')
    db_user = input('\nEnter Database User Name: ')
    db_password = input('\nEnter Database User Password: ')
    db_schema = input('\nEnter Schemas to Export: ')
    cpu_count = input('\nEnter number of cpu: ')
    db_dumpfile = input('\nEnter the Name for Dumpfile (ex. test.dmp): ')

    if db_dumpfile[-4:] != '.dmp':
        db_dumpfile += '.dmp'

    db_logfile = db_dumpfile[:(len(db_dumpfile) - 4)] + ".log"

    db_connect_string = db_user + "/" + db_password + "@" + connect_string(db_ocid)
    schema = " schemas=" + db_schema
    exclude = " exclude=index, cluster, indextype, materialized_view, materialized_view_log, materialized_zonemap, db_link"
    data_option = " data_options=group_partition_table_data"
    parallel = " parallel=" + cpu_count
    dumpfile = " dumpfile=data_pump_dir:" + db_dumpfile
    logfile = " logfile=data_pump_dir:" + db_logfile

    cmd_export = "expdp " + db_connect_string + schema + exclude + data_option + parallel + dumpfile + logfile

    print ('\n\nExporting command: \n' + cmd_export + '\n')

    runcmd(cmd_export)

    print ('\nSuccessfully exported schema {} to {}.'.format(db_schema, '$data_pump_dir'))

    return db_dumpfile, db_logfile




##----- copy dumpfile to oracle object storage for backup -----##

def dumpfile_rclone(db_dumpfile, db_logfile):
    print
    bucket_name = input('\nEnter Object Bucket Name for Storing Dump File: ')
    cmd_rclone_dmp = "sudo rclone sync $DATA_PUMP_DIR/" + db_dumpfile + " remote:" + bucket_name
    cmd_rclone_log = "sudo rclone sync $DATA_PUMP_DIR/" + db_logfile + " remote:" + bucket_name

    print ('\n' + cmd_rclone_dmp)
    print ('\n' + cmd_rclone_log + '\n')
    runcmd(cmd_rclone_dmp)
    runcmd(cmd_rclone_log)

    print ('\nSuccessfully uploaded dump file and log backups to {}.'.format(bucket_name))





def upload_object(db_dumpfile, db_logfile):
    print
    bucket_name = input('Enter Object Bucket Name for Storing Dump File: ')
    data_pump_dir = os.environ['DATA_PUMP_DIR']
    cmd_chmod = "sudo chmod 604 " + data_pump_dir + '/' + db_dumpfile
    cmd_upload_dmp = "oci os object put -bn " + bucket_name + " --file " + data_pump_dir + "/" + db_dumpfile
    cmd_upload_log = "oci os object put -bn " + bucket_name + " --file " + data_pump_dir + "/" + db_logfile

    #print('\n' + cmd_upload_dmp)
    #print('\n' + cmd_upload_log + '\n')

    runcmd(cmd_chmod)
    runcmd(cmd_upload_dmp)
    runcmd(cmd_upload_log)

    print('\nSuccessfully uploaded dump file and log backups to {}.'.format(bucket_name))





def db_import(import_dumpfile):

    adw_name = input('\nEnter Autonomous Data Warehouse Database Name: ')
    adw_user = input('\nEnter Autonomous Data Warehouse User Name: ')
    adw_password = input('\nEnter Autonomous Data Warehouse User Password: ')
    adw_service_name = input('\nEnter Autonomous Data Warehouse Service Name (high, medium, low): ')
    adw_credential = input('\nEnter ADW Credential for Object Storage: ')
    cpu_count = input('\nEnter number of cpu: ')

    adw_connect_string = adw_user + '/' + adw_password + '@' + adw_name + '_' + adw_service_name
    directory = " directory=data_pump_dir"
    credential = " credential=" + adw_credential
    dumpfile = " dumpfile=" + import_dumpfile
    parallel = " parallel=" + cpu_count
    partition_option = " partition_options=merge"
    transform = " transform=segment_attributes:n"
    exclude = " exclude=index, cluster, indextype, materialized_view, materialized_view_log, materialized_zonemap, db_link"

    cmd_import = "impdp " + adw_connect_string + directory + credential + dumpfile + parallel + partition_option + transform + exclude

    print ('\nImporting command: \n' + cmd_import + '\n')

    try:
        runcmd(cmd_import)
    except OSError as e:
        return "Failed: " + str(e)




def db_import_object(object_url):

    adw_name = input('\nEnter Autonomous Data Warehouse Database Name: ')
    adw_user = input('\nEnter Autonomous Data Warehouse User Name: ')
    adw_password = input('\nEnter Autonomous Data Warehouse User Password: ')
    adw_service_name = input('\nEnter Autonomous Data Warehouse Service Name (high, medium, low): ')
    adw_credential = input('\nEnter ADW Credential for Object Storage: ')
    cpu_count = input('\nEnter number of cpu: ')

    adw_connect_string = adw_user + '/' + adw_password + '@' + adw_name + '_' + adw_service_name
    directory=" directory=data_pump_dir"
    credential = " credential=" + adw_credential
    dumpfile = " dumpfile=" + object_url
    parallel = " parallel=" + cpu_count
    partition_option = " partition_options=merge"
    transform = " transform=segment_attributes:n"
    exclude = " exclude=index, cluster, indextype, materialized_view, materialized_view_log, materialized_zonemap, db_link"

    cmd_import = "impdp " + adw_connect_string + directory + credential + dumpfile + parallel + partition_option + transform + exclude

    print ('\nImporting command: \n' + cmd_import + '\n')


    try:
        runcmd(cmd_import)
    except OSError as e:
        return "Failed: " + str(e)





db_new_export = input('\nCreate New Pump File? (yes/y or no/n): ')

if db_new_export.lower() in ['yes', 'y']:

    db_dumpfile, db_logfile = db_export()

    backup_to_obj_str = input('\nMove Pump File Bakcup to Object Storage? (yes/y or no/n): ')

    if backup_to_obj_str.lower() in ['yes', 'y']:
        upload_object(db_dumpfile, db_logfile)

    adw_import = input('\nImport Data to Autonomous Data Warehouse using {} (yes/y) or another dump file (no/n)? : '.format(db_dumpfile))
    if adw_import.lower() in ['yes', 'y']:
        db_import(db_dumpfile)
    else:
        object_url = input('\nEnter the URL of Object File:  ')
        db_import_object(object_url)
else:
    adw_import = input('\nImport Data to Autonomous Data Warehouse? (yes/y or no/n): ')
    if adw_import.lower() in ['yes', 'y']:
        object_url = input('\nEnter the URL of Object File:  ')
        db_import_object(object_url)















