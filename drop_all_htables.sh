#!/bin/bash

# About drop_all_htables.sh 
# =========================
# This script drops all Hive tables from the given database.
#
# Usage: $source drop_all_tables.sh env database-name queue-name
#
#   env: Environment [Required] (dev/int/prod)  
#   database-name: Database name [Optional]. Default will be used if it's not given.
#   queue-name: Queue name [Optional]. If not given, all jobs go to "default" queue.
#
# Example: $source drop_all_tables dev my-db myqueue



# Configurations - Specify below Hive JDBC URL and Kerberos Principal for each environment.
DEV_HIVE2_JDBC_URL=jdbc:hive2://dev-hive.fake.com:25006/default
DEV_KERBEROS_PRINCIPAL=_HOST@DEV-FAKE.COM

INT_HIVE2_JDBC_URL=jdbc:hive2://int-hive.fake.com:25006/default
INT_KERBEROS_PRINCIPAL=_HOST@INT-FAKE.COM

PROD_HIVE2_JDBC_URL=jdbc:hive2://prod-hive.fake.com:25006/default
PROD_KERBEROS_PRINCIPAL=_HOST@PROD-FAKE.COM

DEFAULT_DB=DEFAULT
DEFAULT_QNAME=root.ingestion


print_usage() {
    printf "\nUsage: \$source drop_all_tables.sh env database-name queue-name\n"
    printf "   env: Environment [Required] (dev/int/prod)\n"  
    printf "   database-name: Database name [Optional]. Default will be used if it's not given.\n"
    printf "   queue-name: Queue name [Optional]. If not given, all jobs go to "default" queue.\n\n"
    printf "Example: $sh drop_all_tables dev my-db myqueue\n"
}

set_env_variables() {
    export HIVE2_JDBC_URL=$HIVE2_JDBC_URL
    export KERBEROS_PRINCIPAL=$KERBEROS_PRINCIPAL
}

validate_cli_arguments() {
    # Validates the given environment argument
    if [ -z "${ENV}" ]; then
	echo "Error: No environment specified!"
        print_usage
	exit 1
    fi
         
    case "$ENV" in
    	DEV|INT|PROD) 
	    eval HIVE2_JDBC_URL='$'${ENV}_HIVE2_JDBC_URL
	    eval KERBEROS_PRINCIPAL='$'${ENV}_KERBEROS_PRINCIPAL;;
	*)
	    echo "Error: Invalid environment argument given! (Valid Environments: dev/int/prod)"
            print_usage
	    exit 1;;
    esac


    # Validates the given database argument
    if [ -z "${DB}" ]; then
	# Takes default Database
        DB=$DEFAULT_DB
    fi

    # Validates the given queue name
    if [ -z "${QNAME}" ]; then
	# Takes default Queue name
        QNAME=$DEFAULT_QNAME
    fi

    echo "Hive2 JDBC URL: $HIVE2_JDBC_URL"
    echo "Keberos Principal: $KERBEROS_PRINCIPAL"
    echo "Database: ${DB}"
    echo "Queue Name: ${QNAME}"
}


# Executes beeline statements
run_beeline_stmts() {
    # Check if the given database exists in Hive.
    beeline_out=$(beeline /etc/hive/beeline.properties -u ${HIVE2_JDBC_URL}\;principal=hive/${KERBEROS_PRINCIPAL} --hiveconf mapreduce.job.queuename=${QNAME} --silent=true --showHeader=false --outputformat=csv2 -e 'USE '"$DB" 2>&1)
    if [ $? -ne 0 ]; then
        echo $beeline_out
        exit 1
    fi

    # Drop all tables
    beeline /etc/hive/beeline.properties -u ${HIVE2_JDBC_URL}\;principal=hive/${KERBEROS_PRINCIPAL} --hiveconf mapreduce.job.queuename=${QNAME} --silent=true --showHeader=false --outputformat=csv2 -e 'SHOW TABLES IN '"$DB" | xargs -I '{}' \
    beeline /etc/hive/beeline.properties -u ${HIVE2_JDBC_URL}\;principal=hive/${KERBEROS_PRINCIPAL} --hiveconf mapreduce.job.queuename=${QNAME} --silent=true --showHeader=false --outputformat=csv2 -e 'DROP TABLE IF EXISTS '"$DB".{}
    if [ $? -ne 0 ]; then
        echo "Error encountered while dropping tables!"
        exit 1
    fi
}


main() {
    echo "Running drop-all-tables script..." 
    validate_cli_arguments
    set_env_variables
    run_beeline_stmts
}


# Command-line arguments
ENV="${1^^}"
DB="${2}" 	# Optional. If not given, default database will be used.
QNAME="${3}" 	# Optional. If not given, all jobs go to "default" queue.


main
exit 0
