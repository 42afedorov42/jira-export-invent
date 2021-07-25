#!/usr/bin/python3
import os
from datetime import datetime
from dotenv import load_dotenv
import psycopg2


load_dotenv('.env')
datetime_export = datetime.strftime(datetime.now(), '%Y-%m-%d')
export_path = os.path.abspath(os.curdir)
JIRA_DATABASE = os.getenv('JIRA_DATABASE')
JIRA_DATABASE_USER = os.getenv('JIRA_DATABASE_USER')
JIRA_DATABASE_PASSWORD = os.getenv('JIRA_DATABASE_PASSWORD')
JIRA_DATABASE_HOST = os.getenv('JIRA_DATABASE_HOST')
JIRA_DATABASE_PORT = os.getenv('JIRA_DATABASE_PORT')


connection = psycopg2.connect(
    database=JIRA_DATABASE,
    user=JIRA_DATABASE_USER,
    password=JIRA_DATABASE_PASSWORD,
    host=JIRA_DATABASE_HOST,
    port=JIRA_DATABASE_PORT)


with connection.cursor() as cursor:
    """Get all projects"""
    cursor.execute(
        """SELECT "NAME"
        FROM public."AO_8542F1_IFJ_OBJ"
        WHERE "OBJECT_TYPE_ID" = 166;"""
    )
    projects = [list(row) for row in cursor.fetchall()]
    for project in projects:
        project = project[0]
        """All project equipment in a separate report"""
        cursor.execute(
            f""" COPY (
            /*Column Office*/
            SELECT
                "OfficeTable"."Office", "UseInvDevSerPriTable"."User",
                "UseInvDevSerPriTable"."Invent Number", "UseInvDevSerPriTable"."Device",
                "UseInvDevSerPriTable"."Serial Number", "UseInvDevSerPriTable"."Price"
            FROM
                (SELECT
                    "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID",
                    "AO_8542F1_IFJ_OBJ_ATTR_VAL"."TEXT_VALUE" AS "Office"
                FROM "AO_8542F1_IFJ_OBJ_ATTR"
                INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                    ON "AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"="AO_8542F1_IFJ_OBJ_ATTR"."ID"
                INNER JOIN "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                    ON "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"="AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                WHERE
                    "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"
                    IN (SELECT "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                        FROM "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                        WHERE "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."NAME"='Office')
                ) AS "OfficeTable"

            RIGHT JOIN

            (SELECT
                "UsersTable"."OBJECT_ID",
                "UsersTable"."User",
                "InvDevSerPriTable"."Invent Number",
                "InvDevSerPriTable"."Device",
                "InvDevSerPriTable"."Serial Number",
                "InvDevSerPriTable"."Price"
            FROM
                /*Column User*/
                (SELECT
                    "AO_8542F1_IFJ_OBJ"."NAME" AS "User", "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID"
                FROM "AO_8542F1_IFJ_OBJ"
                INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                    ON "AO_8542F1_IFJ_OBJ"."ID"="AO_8542F1_IFJ_OBJ_ATTR_VAL"."REFERENCED_OBJECT_ID"
                INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR"
                    ON "AO_8542F1_IFJ_OBJ_ATTR"."ID"="AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"
                WHERE
                    "AO_8542F1_IFJ_OBJ_ATTR_VAL"."REFERENCED_OBJECT_ID"
                        IN (SELECT "AO_8542F1_IFJ_OBJ_ATTR_VAL"."REFERENCED_OBJECT_ID"
                            FROM "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                            WHERE "AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"
                                IN (SELECT "AO_8542F1_IFJ_OBJ_ATTR"."ID"
                                    FROM "AO_8542F1_IFJ_OBJ_ATTR"
                                    WHERE "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"
                                        IN (SELECT "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                                            FROM "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                                            WHERE "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."NAME"='User' )))
                ) AS "UsersTable"

            RIGHT JOIN

            (SELECT
                "InventNumberTable"."OBJECT_ID",
                "InventNumberTable"."Invent Number",
                "DevSerPriTable"."Device",
                "DevSerPriTable"."Serial Number",
                "DevSerPriTable"."Price"
            FROM
                /*Column Invent Number*/
                (SELECT
                    "AO_8542F1_IFJ_OBJ_ATTR_VAL"."TEXT_VALUE" AS "Invent Number",
                    "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID"
                FROM "AO_8542F1_IFJ_OBJ_ATTR"
                INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                    ON "AO_8542F1_IFJ_OBJ_ATTR"."ID"="AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"
                INNER JOIN "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                    ON "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"="AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                WHERE
                    "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID"
                        IN (SELECT "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID"
                        FROM "AO_8542F1_IFJ_OBJ_ATTR"
                        INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                            ON "AO_8542F1_IFJ_OBJ_ATTR"."ID"="AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"
                        WHERE "AO_8542F1_IFJ_OBJ_ATTR_VAL"."REFERENCED_OBJECT_ID"
                            IN (SELECT "AO_8542F1_IFJ_OBJ"."ID"
                                FROM "AO_8542F1_IFJ_OBJ"
                                WHERE "AO_8542F1_IFJ_OBJ"."NAME"='{project}'))
                    AND "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"
                        IN (SELECT "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                        FROM "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                        WHERE "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."NAME"='Invent number')
                ) AS "InventNumberTable"

            LEFT JOIN

            (SELECT "DeviceTable"."OBJECT_ID",
                "DeviceTable"."Device",
                "SerPriTable"."Serial Number",
                "SerPriTable"."Price"
            FROM
                /*Column Device*/
                (SELECT
                    "AO_8542F1_IFJ_OBJ_ATTR_VAL"."TEXT_VALUE" AS "Device",
                    "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID"
                FROM "AO_8542F1_IFJ_OBJ_ATTR"
                INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                    ON "AO_8542F1_IFJ_OBJ_ATTR"."ID"="AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"
                INNER JOIN "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                    ON "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"="AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                WHERE "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID"
                    IN (SELECT "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID"
                        FROM "AO_8542F1_IFJ_OBJ_ATTR"
                        INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                            ON "AO_8542F1_IFJ_OBJ_ATTR"."ID"="AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"
                            WHERE "AO_8542F1_IFJ_OBJ_ATTR_VAL"."REFERENCED_OBJECT_ID"
                                IN (SELECT "AO_8542F1_IFJ_OBJ"."ID"
                                    FROM "AO_8542F1_IFJ_OBJ"
                                    WHERE "AO_8542F1_IFJ_OBJ"."NAME"='{project}'))
                    AND "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"
                        IN (SELECT "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                            FROM "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                            WHERE "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."NAME"='Model')
                ) AS "DeviceTable"

            FULL OUTER JOIN

            (SELECT "SerialNumberTable"."OBJECT_ID",
                "SerialNumberTable"."Serial Number",
                "PriceTable"."Price"
            FROM
                /*Column Serial Number*/
                (SELECT
                    "AO_8542F1_IFJ_OBJ_ATTR_VAL"."TEXT_VALUE" AS "Serial Number",
                    "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID"
                FROM "AO_8542F1_IFJ_OBJ_ATTR"
                INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                    ON "AO_8542F1_IFJ_OBJ_ATTR"."ID"="AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"
                INNER JOIN "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                    ON "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"="AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                WHERE "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID"
                    IN (SELECT "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID"
                        FROM "AO_8542F1_IFJ_OBJ_ATTR"
                        INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                            ON "AO_8542F1_IFJ_OBJ_ATTR"."ID"="AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"
                        WHERE "AO_8542F1_IFJ_OBJ_ATTR_VAL"."REFERENCED_OBJECT_ID"
                            IN (SELECT "AO_8542F1_IFJ_OBJ"."ID"
                                FROM "AO_8542F1_IFJ_OBJ"
                                WHERE "AO_8542F1_IFJ_OBJ"."NAME"='{project}'))
                    AND "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"
                        IN (SELECT "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                            FROM "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                            WHERE "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."NAME"='S/N'
                            OR "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."NAME"='MAC/SN')
                ) AS "SerialNumberTable"

            LEFT JOIN

            /*Column Price*/
            (SELECT
                "AO_8542F1_IFJ_OBJ_ATTR_VAL"."DOUBLE_VALUE" AS "Price",
                "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID"
            FROM "AO_8542F1_IFJ_OBJ_ATTR"
            INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                ON "AO_8542F1_IFJ_OBJ_ATTR"."ID"="AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"
            INNER JOIN "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                ON "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"="AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
            WHERE "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID"
                IN (SELECT "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID"
                    FROM "AO_8542F1_IFJ_OBJ_ATTR"
                    INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                        ON "AO_8542F1_IFJ_OBJ_ATTR"."ID"="AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"
                    WHERE "AO_8542F1_IFJ_OBJ_ATTR_VAL"."REFERENCED_OBJECT_ID"
                        IN (SELECT "AO_8542F1_IFJ_OBJ"."ID"
                            FROM "AO_8542F1_IFJ_OBJ"
                            WHERE "AO_8542F1_IFJ_OBJ"."NAME"='{project}'))
            AND "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"
                IN (SELECT "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                    FROM "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                    WHERE "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."NAME"='Price')
            ) AS "PriceTable"

            ON "SerialNumberTable"."OBJECT_ID"="PriceTable"."OBJECT_ID") AS "SerPriTable"
            ON "DeviceTable"."OBJECT_ID"="SerPriTable"."OBJECT_ID") AS "DevSerPriTable"
            ON "InventNumberTable"."OBJECT_ID"="DevSerPriTable"."OBJECT_ID") AS "InvDevSerPriTable"
            ON "UsersTable"."OBJECT_ID"="InvDevSerPriTable"."OBJECT_ID") AS "UseInvDevSerPriTable"
            ON "OfficeTable"."OBJECT_ID"="UseInvDevSerPriTable"."OBJECT_ID"

            ORDER BY "User" ASC
            ) TO '{export_path}{datetime_export}_{project}.csv' WITH (FORMAT CSV, HEADER);"""
        )

    """All equipment from all projects in one report"""
    cursor.execute(
        f""" COPY (
        /*Office*/
        SELECT
            "OfficeTable"."Office",
            "ProUseDevInvSerPriTable"."Project",
            "ProUseDevInvSerPriTable"."User",
            "ProUseDevInvSerPriTable"."Device",
            "ProUseDevInvSerPriTable"."Invent Number",
            "ProUseDevInvSerPriTable"."Serial Number",
            "ProUseDevInvSerPriTable"."Price"
        FROM
            (SELECT
                "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID",
                "AO_8542F1_IFJ_OBJ_ATTR_VAL"."TEXT_VALUE" AS "Office"
            FROM "AO_8542F1_IFJ_OBJ_ATTR"
            INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                ON "AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"="AO_8542F1_IFJ_OBJ_ATTR"."ID"
            INNER JOIN "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                ON "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"="AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
            WHERE
            "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"
                IN (SELECT "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                    FROM "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                    WHERE "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."NAME"='Office')
            ) AS "OfficeTable"

        RIGHT JOIN

        /*Project*/
        (SELECT
            "UseDevInvSerPriTable"."OBJECT_ID",
            "ProjectsTable"."Project",
            "UseDevInvSerPriTable"."User",
            "UseDevInvSerPriTable"."Device",
            "UseDevInvSerPriTable"."Invent Number",
            "UseDevInvSerPriTable"."Serial Number",
            "UseDevInvSerPriTable"."Price"
         FROM
            (SELECT
                "AO_8542F1_IFJ_OBJ"."NAME" AS "Project",
                "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID"
            FROM "AO_8542F1_IFJ_OBJ"
            INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                ON "AO_8542F1_IFJ_OBJ"."ID"="AO_8542F1_IFJ_OBJ_ATTR_VAL"."REFERENCED_OBJECT_ID"
            INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR"
                ON "AO_8542F1_IFJ_OBJ_ATTR"."ID"="AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"
            WHERE "AO_8542F1_IFJ_OBJ_ATTR_VAL"."REFERENCED_OBJECT_ID"
                IN (SELECT "AO_8542F1_IFJ_OBJ_ATTR_VAL"."REFERENCED_OBJECT_ID"
                    FROM "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                    WHERE "AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"
                        IN (SELECT "AO_8542F1_IFJ_OBJ_ATTR"."ID"
                            FROM "AO_8542F1_IFJ_OBJ_ATTR"
                            WHERE "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"
                                IN (SELECT "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                                    FROM "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                                    WHERE "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."NAME"='Project')))
            ) AS "ProjectsTable"

        RIGHT JOIN

        /*User*/
        (SELECT
            "DevInvSerPriTable"."OBJECT_ID",
            "UsersTable"."User", "DevInvSerPriTable"."Device",
            "DevInvSerPriTable"."Invent Number",
            "DevInvSerPriTable"."Serial Number",
            "DevInvSerPriTable"."Price"
         FROM
            (SELECT
                "AO_8542F1_IFJ_OBJ"."NAME" AS "User",
                "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID"
            FROM "AO_8542F1_IFJ_OBJ"
            INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                ON "AO_8542F1_IFJ_OBJ"."ID"="AO_8542F1_IFJ_OBJ_ATTR_VAL"."REFERENCED_OBJECT_ID"
            INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR"
                ON "AO_8542F1_IFJ_OBJ_ATTR"."ID"="AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"
            WHERE "AO_8542F1_IFJ_OBJ_ATTR_VAL"."REFERENCED_OBJECT_ID"
                IN (SELECT "AO_8542F1_IFJ_OBJ_ATTR_VAL"."REFERENCED_OBJECT_ID"
                    FROM "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                    WHERE "AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"
                        IN (SELECT "AO_8542F1_IFJ_OBJ_ATTR"."ID"
                            FROM "AO_8542F1_IFJ_OBJ_ATTR"
                            WHERE "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"
                                IN (SELECT "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                                    FROM "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                                    WHERE "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."NAME"="User")))
            ) AS "UsersTable"

        RIGHT JOIN

        /*Model*/
        (SELECT
            "InvSerPriTable"."OBJECT_ID",
            "DevicesTable"."Device",
            "InvSerPriTable"."Invent Number",
            "InvSerPriTable"."Serial Number",
            "InvSerPriTable"."Price"
        FROM
            (SELECT
                "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID",
                "AO_8542F1_IFJ_OBJ_ATTR_VAL"."TEXT_VALUE" AS "Device"
            FROM "AO_8542F1_IFJ_OBJ_ATTR"
            INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                ON "AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"="AO_8542F1_IFJ_OBJ_ATTR"."ID"
            INNER JOIN "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                ON "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"="AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
            WHERE "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"
                IN (SELECT "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                    FROM "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                    WHERE "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."NAME"='Model')
            ) AS "DevicesTable"

        RIGHT JOIN

        /*Invent Number*/
        (SELECT
            "InventNumberTable"."OBJECT_ID",
            "InventNumberTable"."Invent Number",
            "SerPriTable"."Serial Number",
            "SerPriTable"."Price"
        FROM
            (SELECT
                "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID",
                "AO_8542F1_IFJ_OBJ_ATTR_VAL"."TEXT_VALUE" AS "Invent Number"
            FROM "AO_8542F1_IFJ_OBJ_ATTR"
            INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                ON "AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"="AO_8542F1_IFJ_OBJ_ATTR"."ID"
            INNER JOIN "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                ON "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"="AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
            WHERE "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"
                IN (SELECT "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                    FROM "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                    WHERE "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."NAME"='Invent number')
            ) AS "InventNumberTable"

        LEFT JOIN

        /*Serial Number*/
        (SELECT
            "SerialNumberTable"."OBJECT_ID",
            "SerialNumberTable"."Serial Number",
            "PriceTable"."Price"
        FROM
            (SELECT
                "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID",
                "AO_8542F1_IFJ_OBJ_ATTR_VAL"."TEXT_VALUE" AS "Serial Number"
            FROM "AO_8542F1_IFJ_OBJ_ATTR"
            INNER JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
                ON "AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"="AO_8542F1_IFJ_OBJ_ATTR"."ID"
            INNER JOIN "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                ON "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"="AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
            WHERE "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"
                IN (SELECT "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                    FROM "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                    WHERE "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."NAME"='S/N'
                    OR "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."NAME"='MAC/SN')
            ) AS "SerialNumberTable"

        LEFT JOIN

        /*Price*/
        (SELECT
            "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_ID",
            "AO_8542F1_IFJ_OBJ_ATTR_VAL"."DOUBLE_VALUE" AS "Price"
        FROM "AO_8542F1_IFJ_OBJ_ATTR"
        LEFT JOIN "AO_8542F1_IFJ_OBJ_ATTR_VAL"
            ON "AO_8542F1_IFJ_OBJ_ATTR_VAL"."OBJECT_ATTRIBUTE_ID"="AO_8542F1_IFJ_OBJ_ATTR"."ID"
        INNER JOIN "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
            ON "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"="AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
        WHERE "AO_8542F1_IFJ_OBJ_ATTR"."OBJECT_TYPE_ATTRIBUTE_ID"
            IN (SELECT "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."ID"
                FROM "AO_8542F1_IFJ_OBJ_TYPE_ATTR"
                WHERE "AO_8542F1_IFJ_OBJ_TYPE_ATTR"."NAME"='Price')
        ) AS "PriceTable"

        ON "SerialNumberTable"."OBJECT_ID"="PriceTable"."OBJECT_ID") AS "SerPriTable"
        ON "InventNumberTable"."OBJECT_ID"="SerPriTable"."OBJECT_ID") AS "InvSerPriTable"
        ON "DevicesTable"."OBJECT_ID"="InvSerPriTable"."OBJECT_ID") AS "DevInvSerPriTable"
        ON "UsersTable"."OBJECT_ID"="DevInvSerPriTable"."OBJECT_ID") AS "UseDevInvSerPriTable"
        ON "ProjectsTable"."OBJECT_ID"="UseDevInvSerPriTable"."OBJECT_ID") AS "ProUseDevInvSerPriTable"
        ON "OfficeTable"."OBJECT_ID"="ProUseDevInvSerPriTable"."OBJECT_ID"

        ORDER BY "Project" ASC
        ) TO '{export_path}{datetime_export}_All_devices.csv' WITH (FORMAT CSV, HEADER);"""
    )
