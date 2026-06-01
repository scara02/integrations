import os
import re
import struct
from datetime import datetime, timezone

import pandas as pd
import pyodbc
from azure.identity import ClientSecretCredential
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


SQL_COPT_SS_ACCESS_TOKEN = 1256


class FabricToInfluxSync:
    def __init__(self):
        self.fabric_sql_server = self._get_required_env("FABRIC_SQL_SERVER")
        self.fabric_sql_database = self._get_required_env("FABRIC_SQL_DATABASE")
        self.fabric_table = os.getenv("FABRIC_TABLE", "weather_nyc_hourly")
        self.lookback_hours = int(os.getenv("LOOKBACK_HOURS", "48"))

        self.azure_tenant_id = self._get_required_env("AZURE_TENANT_ID")
        self.azure_client_id = self._get_required_env("AZURE_CLIENT_ID")
        self.azure_client_secret = self._get_required_env("AZURE_CLIENT_SECRET")

        self.influxdb_url = self._get_required_env("INFLUXDB_URL")
        self.influxdb_token = self._get_required_env("INFLUXDB_TOKEN")
        self.influxdb_org = self._get_required_env("INFLUXDB_ORG")
        self.influxdb_bucket = self._get_required_env("INFLUXDB_BUCKET")

        self.measurement = "weather_nyc_hourly"

    def _get_required_env(self, name):
        value = os.getenv(name)
        if not value:
            raise ValueError(f"Missing required environment variable: {name}")
        return value

    def _validate_table_name(self):
        pattern = r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$"

        if not re.match(pattern, self.fabric_table):
            raise ValueError(f"Invalid table name: {self.fabric_table}")

    def _get_fabric_access_token(self):
        credential = ClientSecretCredential(
            tenant_id=self.azure_tenant_id,
            client_id=self.azure_client_id,
            client_secret=self.azure_client_secret,
        )

        token = credential.get_token("https://database.windows.net/.default").token
        token_bytes = token.encode("utf-16-le")

        return struct.pack(
            f"<I{len(token_bytes)}s",
            len(token_bytes),
            token_bytes
        )

    def _connect_to_fabric_sql_endpoint(self):
        access_token = self._get_fabric_access_token()

        connection_string = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={self.fabric_sql_server};"
            f"DATABASE={self.fabric_sql_database};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )

        return pyodbc.connect(
            connection_string,
            attrs_before={SQL_COPT_SS_ACCESS_TOKEN: access_token}
        )

    def extract_weather_data(self):
        self._validate_table_name()

        query = f"""
            SELECT *
            FROM {self.fabric_table}
            WHERE datetime_utc >= DATEADD(hour, -?, SYSUTCDATETIME())
            ORDER BY datetime_utc;
        """

        with self._connect_to_fabric_sql_endpoint() as conn:
            df = pd.read_sql(query, conn, params=[self.lookback_hours])

        print(f"Extracted rows from Fabric: {len(df)}")

        if not df.empty:
            print("Min datetime:", df["datetime_utc"].min())
            print("Max datetime:", df["datetime_utc"].max())

        return df

    def _row_to_influx_point(self, row):
        timestamp = pd.to_datetime(row["datetime_utc"], utc=True)

        point = (
            Point(self.measurement)
            .tag("city", str(row.get("city", "New York City")))
            .tag("source", str(row.get("source", "Fabric")))
            .time(timestamp.to_pydatetime(), WritePrecision.NS)
        )

        field_columns = [
            "latitude",
            "longitude",
            "temp",
            "dwpt",
            "rhum",
            "prcp",
            "snow",
            "wdir",
            "wspd",
            "wpgt",
            "pres",
            "tsun",
            "coco",
        ]

        has_field = False

        for col in field_columns:
            if col not in row.index:
                continue

            value = row[col]

            if pd.isna(value):
                continue

            if col == "coco":
                point = point.field(col, int(value))
            else:
                point = point.field(col, float(value))

            has_field = True

        if not has_field:
            return None

        return point

    def load_to_influxdb(self, df):
        if df.empty:
            print("No rows to write to InfluxDB.")
            return 0

        points = []

        for _, row in df.iterrows():
            point = self._row_to_influx_point(row)

            if point is not None:
                points.append(point)

        if not points:
            print("No valid InfluxDB points created.")
            return 0

        with InfluxDBClient(
            url=self.influxdb_url,
            token=self.influxdb_token,
            org=self.influxdb_org,
        ) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            write_api.write(
                bucket=self.influxdb_bucket,
                org=self.influxdb_org,
                record=points,
            )

        print(f"Written points to InfluxDB: {len(points)}")
        return len(points)

    def run(self):
        print("Starting Fabric → InfluxDB sync")
        print("UTC now:", datetime.now(timezone.utc).isoformat())
        print("Lookback hours:", self.lookback_hours)

        df = self.extract_weather_data()
        rows_written = self.load_to_influxdb(df)

        print("Sync completed.")
        print("Rows written:", rows_written)


if __name__ == "__main__":
    FabricToInfluxSync().run()