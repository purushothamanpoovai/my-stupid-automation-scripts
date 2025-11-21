#!/usr/bin/env python3
"""
MySQL → Hive + Parquet Schema Converter
Version: v4 (Optimized and Secure)

Usage:
    mysql2ddl_v4.py --host HOST --user USER [--password PASS | --ask-pass]
                    --database DB --table table1,table2
                    [--hive hive.sql] [--parquet parquet.sql]
                    [--verbose] [--strict]

Features:
 - Multi-table conversion (--table table1,table2,...)
 - Secure password handling (--ask-pass)
 - Global clean type mapping dictionary
 - Warning messages for unsupported or unknown data types
 - Automatic primary key detection
 - Colorized verbose output
 - Strict mode: aborts on unmapped types
 - Generates Hive DDL and/or Parquet (PyArrow) schema
 - Open-source friendly, easy to extend
"""

import argparse
import getpass
import pymysql
import re
import sys
import pyarrow as pa

# ==============================================================================
# COLOR OUTPUT HELPERS
# ==============================================================================

class COLOR:
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    RESET = "\033[0m"


def info(msg, enabled=False):
    if enabled:
        print(COLOR.BLUE + msg + COLOR.RESET)


def ok(msg, enabled=False):
    if enabled:
        print(COLOR.GREEN + msg + COLOR.RESET)


def warn(msg):
    print(COLOR.YELLOW + msg + COLOR.RESET)


def error(msg):
    print(COLOR.RED + msg + COLOR.RESET)


# ==============================================================================
# GLOBAL MYSQL → HIVE → PARQUET TYPE MAP
# ==============================================================================

MYSQL_TYPE_MAP = {
    r"tinyint\(1\)": {
        "hive": "BOOLEAN",
        "parquet": pa.bool_(),
        "note": "Mapped MySQL tinyint(1) to BOOLEAN"
    },
    r"tinyint": {
        "hive": "TINYINT",
        "parquet": pa.int8(),
        "note": ""
    },
    r"smallint": {
        "hive": "SMALLINT",
        "parquet": pa.int16(),
        "note": ""
    },
    r"mediumint": {
        "hive": "INT",
        "parquet": pa.int32(),
        "note": "MySQL MEDIUMINT mapped to Hive INT"
    },
    r"int": {
        "hive": "INT",
        "parquet": pa.int32(),
        "note": ""
    },
    r"bigint": {
        "hive": "BIGINT",
        "parquet": pa.int64(),
        "note": ""
    },
    r"float": {
        "hive": "FLOAT",
        "parquet": pa.float32(),
        "note": ""
    },
    r"double": {
        "hive": "DOUBLE",
        "parquet": pa.float64(),
        "note": ""
    },
    r"decimal\((\d+),(\d+)\)": {
        "hive": lambda p, s: f"DECIMAL({p},{s})",
        "parquet": lambda p, s: pa.decimal128(int(p), int(s)),
        "note": ""
    },
    r"varchar\(\d+\)": {
        "hive": "STRING",
        "parquet": pa.string(),
        "note": ""
    },
    r"char\(\d+\)": {
        "hive": "STRING",
        "parquet": pa.string(),
        "note": ""
    },
    r"text": {
        "hive": "STRING",
        "parquet": pa.string(),
        "note": ""
    },
    r"blob": {
        "hive": "BINARY",
        "parquet": pa.binary(),
        "note": ""
    },
    r"date": {
        "hive": "DATE",
        "parquet": pa.date32(),
        "note": ""
    },
    r"datetime": {
        "hive": "TIMESTAMP",
        "parquet": pa.timestamp("s"),
        "note": ""
    },
    r"timestamp": {
        "hive": "TIMESTAMP",
        "parquet": pa.timestamp("s"),
        "note": ""
    },

    # Special case types
    r"enum": {
        "hive": "STRING",
        "parquet": pa.string(),
        "note": "ENUM is not supported in Hive; converted to STRING"
    },
    r"set": {
        "hive": "STRING",
        "parquet": pa.string(),
        "note": "SET is not supported in Hive; converted to STRING"
    },
    r"json": {
        "hive": "STRING",
        "parquet": pa.string(),
        "note": "JSON stored as STRING"
    },
    r"time": {
        "hive": "STRING",
        "parquet": pa.string(),
        "note": "TIME is unsupported; stored as STRING"
    },
    r"year": {
        "hive": "INT",
        "parquet": pa.int32(),
        "note": "YEAR mapped to INT"
    },
    r"geometry": {
        "hive": "STRING",
        "parquet": pa.string(),
        "note": "GEOMETRY mapped to STRING (WKT representation expected)"
    }
}


# ==============================================================================
# TYPE MATCHING LOGIC
# ==============================================================================

def convert_mysql_type(colname, mysql_type, strict=False, verbose=False):
    t = mysql_type.lower().strip()

    for pattern, mapping in MYSQL_TYPE_MAP.items():
        m = re.match(pattern, t)
        if m:
            hive_type = mapping["hive"]
            pq_type = mapping["parquet"]

            # DECIMAL case
            if callable(hive_type):
                p, s = m.groups()
                hive_type = hive_type(p, s)
                pq_type = pq_type(p, s)

            if mapping["note"] and verbose:
                info(f"{colname}: {mapping['note']}", True)

            return hive_type, pq_type

    # Unknown type
    msg = f"Unknown MySQL type '{mysql_type}' for column '{colname}'"
    if strict:
        error(msg + " (strict mode enabled)")
        sys.exit(1)

    warn(msg + ". Using STRING fallback.")
    return "STRING", pa.string()


# ==============================================================================
# DDL EXTRACTION + PARSING
# ==============================================================================

def extract_columns(mysql_ddl, strict=False, verbose=False):
    columns = []
    primary_keys = []

    for line in mysql_ddl.split("\n"):
        line = line.strip().rstrip(",")

        if line.startswith("`"):
            parts = line.split()
            colname = parts[0].strip("`")
            coltype = parts[1]
            hive_type, pq_type = convert_mysql_type(colname, coltype, strict, verbose)
            columns.append((colname, hive_type, pq_type))

        if line.upper().startswith("PRIMARY KEY"):
            pk_cols = re.findall(r"`([^`]+)`", line)
            primary_keys.extend(pk_cols)

    return columns, primary_keys


# ==============================================================================
# GENERATE OUTPUTS
# ==============================================================================

def generate_hive_ddl(tbl, columns, pks):
    ddl = f"CREATE TABLE {tbl} (\n"
    lines = []

    for col, hive_type, _ in columns:
        if col in pks:
            lines.append(f"  `{col}` {hive_type} -- PRIMARY KEY")
        else:
            lines.append(f"  `{col}` {hive_type}")

    ddl += ",\n".join(lines)
    ddl += "\n)\nSTORED AS PARQUET;\n"
    return ddl


def generate_parquet_schema(tbl, columns):
    out = [f"{tbl}_schema = pa.schema(["]
    for col, _, pq_type in columns:
        out.append(f'    ("{col}", pa.{pq_type}),')
    out.append("])\n")
    return "\n".join(out)


# ==============================================================================
# INPUT VALIDATION
# ==============================================================================

def validate_args(args):
    if not args.hive and not args.parquet:
        error("Either --hive or --parquet must be provided.")
        sys.exit(1)

    if args.password and args.ask_pass:
        error("Cannot use both --password and --ask-pass.")
        sys.exit(1)

    if not args.table:
        error("You must provide at least one table using --table.")
        sys.exit(1)


# ==============================================================================
# MAIN LOGIC
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="MySQL → Hive + Parquet DDL Converter"
    )

    parser.add_argument("--host", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password")
    parser.add_argument("--ask-pass", action="store_true",
                        help="Prompt for MySQL password securely")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--database", required=True)

    parser.add_argument("--table", required=True, help="Comma-separated tables")

    parser.add_argument("--hive", help="Hive DDL output file")
    parser.add_argument("--parquet", help="Parquet schema output file")

    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--strict", action="store_true",
                        help="Strict mode: abort on unknown types")

    args = parser.parse_args()
    validate_args(args)

    # Secure password prompt
    if args.ask_pass:
        args.password = getpass.getpass("Enter MySQL password: ")

    tables = [t.strip() for t in args.table.split(",")]

    info("Connecting to MySQL...", args.verbose)

    try:
        conn = pymysql.connect(
            host=args.host,
            user=args.user,
            password=args.password,
            database=args.database,
            port=args.port
        )
    except Exception as e:
        error(f"Connection failed: {e}")
        sys.exit(1)

    ok("Connected.", args.verbose)

    with conn:
        for tbl in tables:
            info(f"Processing table: {tbl}", args.verbose)

            with conn.cursor() as cur:
                cur.execute(f"SHOW CREATE TABLE `{tbl}`;")
                res = cur.fetchone()

                if not res:
                    warn(f"Table not found: {tbl}")
                    continue

                mysql_ddl = res[1]

            columns, pk_cols = extract_columns(mysql_ddl, args.strict, args.verbose)

            # Hive Output
            if args.hive:
                hive_ddl = generate_hive_ddl(tbl, columns, pk_cols)
                with open(args.hive, "a") as f:
                    f.write(hive_ddl + "\n\n")
                ok(f"Hive DDL written to: {args.hive}", args.verbose)

            # Parquet Output
            if args.parquet:
                pq_ddl = generate_parquet_schema(tbl, columns)
                with open(args.parquet, "a") as f:
                    f.write(pq_ddl + "\n")
                ok(f"Parquet schema written to: {args.parquet}", args.verbose)

    ok("Completed all tasks.", True)


if __name__ == "__main__":
    main()
